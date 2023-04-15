package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const maxBatchSize = 5
const batchInterval = 250 * time.Millisecond

type State struct {
	sendNeighbors              []string
	messages                   []any
	seenMessages               map[any]struct{}
	maxBatchSize               int
	batch                      []any
	mu                         *sync.Mutex
	abortChannel               chan string
	circuitBreakersPerNeighbor map[string]func(thunk func(success *bool) error) func(success *bool) error
}

func NewState() *State {
	return &State{
		sendNeighbors:              make([]string, 0),
		messages:                   make([]any, 0),
		seenMessages:               make(map[any]struct{}),
		batch:                      make([]any, 0, maxBatchSize),
		maxBatchSize:               maxBatchSize,
		mu:                         &sync.Mutex{},
		abortChannel:               make(chan string),
		circuitBreakersPerNeighbor: make(map[string]func(thunk func(success *bool) error) func(success *bool) error),
	}
}

/** Meant to be used in a goroutine */
func tickerBatchRPC(n *maelstrom.Node, state *State) {
	t := time.NewTicker(batchInterval)
	for {
		select {
		// Abort if a request has been sent from the broadcast handler
		case <-state.abortChannel:
			t.Reset(batchInterval)

		// Otherwise, we have waited enough to send a batch request
		case <-t.C:
			state.mu.Lock()

			if len(state.batch) == 0 {
				state.mu.Unlock()
				continue
			}

			// deepcopy state.batch to snapshot the state. Do this with the mutex locked
			currentBatch := make([]any, len(state.batch))
			copy(currentBatch, state.batch)

			// clean state.batch
			state.batch = make([]any, 0, maxBatchSize)

			state.mu.Unlock()

			// We could only send if there are new messages but we don't make this optimization
			for _, neighbor := range state.sendNeighbors {
				go sendBroadcastBatchWithRetries(n, neighbor, currentBatch)
			}
		}
	}
}

func sendBroadcastBatchWithRetries(n *maelstrom.Node, neighbor string, messageBatch []any) error {
	retryOptions := &RetryOptions{
		firstRetryDelayMs: 400,
		maxRetryDelayMs:   10_000,
		jitterConstant:    0.2,
	}
	thunk := func(success *bool) error {
		rpcBody := make(map[string]any)
		rpcBody["type"] = "broadcast"
		rpcBody["message"] = messageBatch
		return n.RPC(neighbor, rpcBody, func(msg maelstrom.Message) error {
			*success = true
			return nil
		})
	}
	return WithRetryAsync(retryOptions, thunk)
}

func main() {

	state := NewState()

	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		returnBody := make(map[string]any)
		returnBody["type"] = "broadcast_ok"
		_, hasMultipleMessages := body["message"].([]any)

		if !hasMultipleMessages {
			newMessage := body["message"]
			state.mu.Lock()

			// Short-circuit here if the message has already been seen
			if _, ok := state.seenMessages[newMessage]; ok {
				state.mu.Unlock()
				return n.Reply(msg, returnBody)
			}

			state.seenMessages[newMessage] = struct{}{}
			state.messages = append(state.messages, newMessage)
			state.batch = append(state.batch, newMessage)

			// deepcopy state.batch to snapshot the state with the mutex locked
			currentBatch := make([]any, len(state.batch))
			copy(currentBatch, state.batch)

			// We will send the snapshotted batch so we clear the state.batch array while holding the mutex
			if len(state.batch) >= state.maxBatchSize {
				state.batch = make([]any, 0, state.maxBatchSize)
			}

			state.mu.Unlock()

			// Only send the batch request when there are enough entries
			if len(currentBatch) >= state.maxBatchSize {
				// Stop the background batch sender
				state.abortChannel <- "abort"

				for _, neighbor := range state.sendNeighbors {
					// Send the snapshotted batch
					go sendBroadcastBatchWithRetries(n, neighbor, currentBatch)
				}
			}

			return n.Reply(msg, returnBody)
		} else {
			messageBatch := body["message"].([]any)

			state.mu.Lock()

			allAcked := true
			for _, newMessage := range messageBatch {
				if _, ok := state.seenMessages[newMessage]; !ok {
					allAcked = false
					break
				}
			}

			// Short-circuit here if the all messages have already been seen
			if allAcked {
				state.mu.Unlock()
				return n.Reply(msg, returnBody)
			}

			for _, newMessage := range messageBatch {
				// deduplicate messages
				if _, seen := state.seenMessages[newMessage]; seen {
					continue
				}
				state.seenMessages[newMessage] = struct{}{}
				state.messages = append(state.messages, newMessage)
				state.batch = append(state.batch, newMessage)
			}

			// deepcopy state.batch to snapshot the state with the mutex locked
			currentBatch := make([]any, len(state.batch))
			copy(currentBatch, state.batch)

			// We will send the snapshotted batch so we clear the state.batch array
			if len(state.batch) >= state.maxBatchSize {
				state.batch = make([]any, 0, 2)
			}

			state.mu.Unlock()

			// Only send the batch request when there are enough entries
			if len(currentBatch) >= state.maxBatchSize {
				// Stop the background batch sender
				state.abortChannel <- "abort"

				for _, neighbor := range state.sendNeighbors {
					// Send the snapshotted batch
					go sendBroadcastBatchWithRetries(n, neighbor, currentBatch)
				}
			}

			return n.Reply(msg, returnBody)
		}
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = state.messages

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshall the body
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topology := body["topology"].(map[string]interface{})
		for _, nei := range topology[n.ID()].([]interface{}) {
			// Initialize the state
			neighbor := nei.(string)
			state.mu.Lock()
			state.sendNeighbors = append(state.sendNeighbors, neighbor)
			state.circuitBreakersPerNeighbor[neighbor] = CreateCircuitBreaker(
				&CircuitBreakerOptions{resetTimeout: 30 * time.Second})
			state.mu.Unlock()
		}

		returnBody := make(map[string]any)
		returnBody["type"] = "topology_ok"
		return n.Reply(msg, returnBody)
	})

	go tickerBatchRPC(n, state)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
