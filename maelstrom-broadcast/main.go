package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	sendNeighbors    []string
	receiveNeighbors []string

	neighborAckedMessages map[string]map[any]struct{}

	messages     []any
	seenMessages map[any]struct{}
}

func NewState() *State {
	return &State{
		sendNeighbors: make([]string, 0),
		messages:      make([]any, 0),
		seenMessages:  make(map[any]struct{}),
	}
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

		// Short-circuit here if the message has already been seen
		if _, ok := state.seenMessages[newMessage]; ok {
			return n.Reply(msg, returnBody)
		}

		state.messages = append(state.messages, newMessage)
		state.seenMessages[newMessage] = struct{}{}

		for _, neighbor := range state.sendNeighbors {
			rpcBody := make(map[string]any)
			rpcBody["type"] = "broadcast"
			rpcBody["message"] = newMessage
			defer n.RPC(neighbor, rpcBody, func(msg maelstrom.Message) error {
				var body map[string]any
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}

				return nil
			})
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
			neighbor := nei.(string)
			state.sendNeighbors = append(state.sendNeighbors, neighbor)
		}

		returnBody := make(map[string]any)
		returnBody["type"] = "topology_ok"
		return n.Reply(msg, returnBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
