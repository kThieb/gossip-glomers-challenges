package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var nodeIdString string

	ids := make(map[string]bool)

	n.Handle("init", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		nodeIdString, _ = body["node_id"].(string)

		returnBody := make(map[string]any)
		returnBody["type"] = "init_ok"

		return n.Reply(msg, returnBody)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["id"] = generateId(nodeIdString, ids)
		body["type"] = "generate_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func generateId(nodeId string, ids map[string]bool) string {
	for {
		id := nodeId + uuid.New().String()
		if _, ok := ids[id]; !ok {
			ids[id] = true
			return id
		}
	}
}
