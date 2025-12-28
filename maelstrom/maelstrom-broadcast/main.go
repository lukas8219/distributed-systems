package main

import (
  "encoding/json"
  "log"

  maelstrom "github.com/jepsen-io/maelstrom/demo/go"
  "sync"
)

func main(){
  n := maelstrom.NewNode()
  state := map[float64]struct{}{}
  var neighbors = make([]string, 0)
  var lock sync.RWMutex

  n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
    return nil
  })

  n.Handle("broadcast", func(msg maelstrom.Message) error {
    lock.Lock()
    defer lock.Unlock()
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }

    message := body["message"].(float64)
    if _, ok := state[message]; ok {
      return nil
    }
    state[message] = struct{}{}

    for _, neighbor := range neighbors {
      n.Send(neighbor, map[string]any{ "type": "broadcast", "message": message })
    }

    return n.Reply(msg, map[string]any{
      "type": "broadcast_ok",
    })
  })
  n.Handle("topology", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }

    messageNeighbors := body["topology"].(map[string]any)[n.ID()]

    nnbrs := make([]string, 0)
    for _, v := range messageNeighbors.([]interface{}) {
      nnbrs = append(nnbrs, v.(string))
    }
    neighbors = nnbrs

    return n.Reply(msg, map[string]any{
      "type": "topology_ok",
    })
  })
  n.Handle("read", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }
    messages := make([]float64, len(state))
    for v, _ := range state {
      messages = append(messages, v)
    }
    return n.Reply(msg, map[string]any{
      "type": "read_ok",
      "messages": messages,
    })
  })

  if err := n.Run(); err != nil {
    log.Fatal(err)
  }

}
