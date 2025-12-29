package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Neighbor struct {
  id string
  lastSyncedVersion float64
}

func now() int64 {
  return time.Now().UnixNano()
}

func main(){
  n := maelstrom.NewNode()
  index := map[float64]struct{}{} //this would be reconstructed from the WAL/state variable
  state := make([]float64, 0)
  var neighbors = map[string]*Neighbor{}
  var lock sync.RWMutex

  n.Handle("sync", func(msg maelstrom.Message) error {
    lock.Lock()
    defer lock.Unlock()
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
      return err
    }
    currentState := float64(len(state))
    dt := body["delta"].(float64)
    if currentState == dt {
      // all good
      return nil
    }
    if currentState < dt {
      panic("Invalid state")
    }
    missingMessages := state[int(dt):int(currentState)] 
    return n.Reply(msg, map[string]any {
      "type": "sync_ok",
      "messages_delta": missingMessages,
      "delta": currentState,
    } )
  })
  n.Handle("sync_ok", func(msg maelstrom.Message) error {
    return nil
  })

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
    defer lock.Unlock()
    if _, ok := index[message]; ok {
      return n.Reply(msg, map[string]any{"type":"broadcast_ok"})
    }
    index[message] = struct{}{}
    state = append(state, message)

    for _, neighbor := range neighbors {
      if strings.EqualFold(neighbor.id, msg.Src) {
        continue
      }
      n.Send(neighbor.id, map[string]any{ "type": "broadcast", "message": message })
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

    for _, v := range messageNeighbors.([]any) {
      nnb := v.(string)
      neigh := &Neighbor{ id: nnb, lastSyncedVersion: 0}
      neighbors[nnb] = neigh
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

  bg := context.Background()
  // This is a hard NO
  go func(){
    for range time.Tick(time.Millisecond * 250) {
      for _, nb := range neighbors {
        ctx, cancel := context.WithTimeout(bg, time.Second * 10)
        defer cancel()
        msg, err := n.SyncRPC(ctx, nb.id, map[string]any{
          "type": "sync",
          "delta": nb.lastSyncedVersion,
        })
        if err != nil {
          continue
        }
        lock.Lock()
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
          continue
        }
        deltaMessage := body["messages_delta"].([]any)
        for _, mAny := range deltaMessage {
          m := mAny.(float64)
          if _, ok := index[m]; ok {
            continue
          }
          index[m] = struct{}{}
          state = append(state, m)
        }
        neighbors[msg.Src].lastSyncedVersion = body["delta"].(float64)
        lock.Unlock()
      }
    }
  }()

  if err := n.Run(); err != nil {
    log.Fatal(err)
  }


}
