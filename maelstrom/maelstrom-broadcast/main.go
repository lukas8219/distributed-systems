package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main(){
  n := maelstrom.NewNode()
  var state = make([]float64, 0)

  n.Handle("broadcast", func(msg maelstrom.Message) error {
    // Unmarshal the message body as an loosely-typed map.
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    bodyMessage := body["message"]
    state = append(state, bodyMessage.(float64))
    return n.Reply(msg, map[string]any{
    "type": "broadcast_ok",
    })
  })
  n.Handle("topology", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    return n.Reply(msg, map[string]any{
    "type": "topology_ok",
    })
  })
  n.Handle("read", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }
    return n.Reply(msg, map[string]any{
      "type": "read_ok",
      "messages": state,
    })
  })

  if err := n.Run(); err != nil {
    log.Fatal(err)
  }

}
