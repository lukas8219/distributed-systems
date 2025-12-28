package main

import (
	"encoding/json"
	"log"
	"strings"

	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main(){
  n := maelstrom.NewNode()
  state := map[float64]struct{}{}
  var neighbors = make([]string, 0)
  var lock sync.RWMutex

  n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
    return nil
  })


  /*
I need to create a sync mechanism since `Send` can fail and it doesnt seem like I can use SyncRPC
Maybe fetch neighbors state each ~50ms or something
I can still spend some time in making SyncRPC work (as, although not performant) is the more intuitive way
Rather not optimize before the real challenges
  */

  n.Handle("broadcast", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }

    message := body["message"].(float64)
    lock.Lock()
    if _, ok := state[message]; ok {
      lock.Unlock()
      return n.Reply(msg, map[string]any{"type":"broadcast_ok"})
    }
    state[message] = struct{}{}
    lock.Unlock()

    for _, neighbor := range neighbors {
      if strings.EqualFold(neighbor, msg.Src) {
        continue
      }
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
    for _, v := range messageNeighbors.([]any) {
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
    for v := range state {
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
