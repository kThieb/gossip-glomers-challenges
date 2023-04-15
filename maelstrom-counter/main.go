package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	counterKey = "cnt"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		deltaF := body["delta"].(float64)
		delta := int(deltaF)

		// Atomically add the delta
		for {
			cur, err := kv.ReadInt(ctx, counterKey)
			if err != nil {
				continue
			}

			err = kv.CompareAndSwap(ctx, counterKey, cur, cur+delta, false)
			if err == nil {
				break
			}
		}

		returnBody := make(map[string]any)
		returnBody["type"] = "add_ok"

		return n.Reply(msg, returnBody)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		returnBody := make(map[string]any)
		returnBody["type"] = "read_ok"

		// Compare and swap the same value to read the latest
		var err error
		var v int
		for {
			v, err = kv.ReadInt(ctx, counterKey)
			if err != nil {
				continue
			}

			err = kv.CompareAndSwap(ctx, counterKey, v, v, false)
			if err == nil {
				break
			}
		}
		returnBody["value"] = v

		return n.Reply(msg, returnBody)
	})

	n.Handle("init", func(msg maelstrom.Message) error {
		kv.Write(ctx, counterKey, 0)
		return nil
	})

	if err := n.Run(); err != nil {
		log.Panicln(err)
	}
}
