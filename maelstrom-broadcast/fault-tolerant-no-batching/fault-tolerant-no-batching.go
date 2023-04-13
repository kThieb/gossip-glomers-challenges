package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	sendNeighbors []string
	messages      []any
	seenMessages  map[any]struct{}
	mu            *sync.Mutex
}

func NewState() *State {
	return &State{
		sendNeighbors: make([]string, 0),
		messages:      make([]any, 0),
		seenMessages:  make(map[any]struct{}),

		// The following mutex guards both sendNeighborAckedMessages and inFlightBroadcasts
		mu: new(sync.Mutex),
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sendBroadcastWithRetries(n *maelstrom.Node, neighbor string, message any, state *State) error {
	rpcBody := make(map[string]any)
	rpcBody["type"] = "broadcast"
	rpcBody["message"] = message

	acked := false

	// Retry with capped exponential backoff and jitter
	retryIn := 400
	maxRetryIn := 10000
	jitterConstant := 0.2

	for !acked {
		n.RPC(neighbor, rpcBody, func(msg maelstrom.Message) error {
			var responseBody map[string]any
			if err := json.Unmarshal(msg.Body, &responseBody); err != nil {
				return err
			}

			acked = true

			return nil
		})

		randomMultiplier := 1 - jitterConstant*(rand.Float64()-0.5)
		sleepFor := time.Duration(randomMultiplier * float64(retryIn))
		time.Sleep(sleepFor * time.Millisecond)

		retryIn = min(2*retryIn, maxRetryIn)
	}
	return nil
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
		newMessage := body["message"]

		state.mu.Lock()
		// Short-circuit here if the message has already been seen
		if _, ok := state.seenMessages[newMessage]; ok {
			state.mu.Unlock()
			return n.Reply(msg, returnBody)
		}
		state.seenMessages[newMessage] = struct{}{}
		state.messages = append(state.messages, newMessage)
		state.mu.Unlock()

		for _, neighbor := range state.sendNeighbors {
			if neighbor == msg.Src {
				continue
			}
			go sendBroadcastWithRetries(n, neighbor, newMessage, state)
		}

		return n.Reply(msg, returnBody)
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
			state.mu.Unlock()
		}

		returnBody := make(map[string]any)
		returnBody["type"] = "topology_ok"
		return n.Reply(msg, returnBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
