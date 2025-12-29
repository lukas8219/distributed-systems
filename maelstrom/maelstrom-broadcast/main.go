package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Neighbor struct {
	id                string
	lastSyncedVersion uint64
}

func now() int64 {
	return time.Now().UnixNano()
}

type Sync struct {
	Type  string `json:"type"`
	Delta uint64 `json:"delta"`
}

type SyncOk struct {
	Type          string   `json:"type"`
	MessagesDelta []uint64 `json:"messages_delta"`
	Delta         uint64   `json:"delta"`
}

type Broadcast struct {
	Type    string `json:"type"`
	Message uint64 `json:"message"`
}

type BroadcastOk struct {
	Type      string `json:"type"`
	InReplyTo uint64 `json:"in_reply_to,omitempty"`
}

type Topology struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type TopologyOk struct {
	Type string `json:"type"`
}

type Read struct {
	Type string `json:"type"`
}

type ReadOk struct {
	Type     string   `json:"type"`
	Messages []uint64 `json:"messages"`
}

func main() {
	n := maelstrom.NewNode()
	index := map[uint64]struct{}{} //this would be reconstructed from the WAL/state variable
	state := make([]uint64, 0)
	var neighbors = map[string]*Neighbor{}
	var lock sync.RWMutex

	calculateDelta := func(delta uint64) (uint64, []uint64, error) {
		currentState := uint64(len(state))
		if currentState == delta {
			return 0, []uint64{}, nil
		}
		if currentState < delta {
			return 0, []uint64{}, errors.New("Invalid state, skip")
		}
		missingMessages := state[delta:currentState]
		return uint64(currentState), missingMessages, nil
	}

	sendBatch := func(nbId string, nblastSyncVersion uint64) {
		delta, messages, err := calculateDelta(nblastSyncVersion)
		if err != nil {
			return
		}
		n.Send(nbId, SyncOk{Type: "sync_ok", Delta: delta, MessagesDelta: messages})
	}

	handleSyncOk := func(source string, msg maelstrom.Message) error {
		lock.Lock()
		defer lock.Unlock()
		var syncOk SyncOk
		if err := json.Unmarshal(msg.Body, &syncOk); err != nil {
			return err
		}
		deltaMessage := syncOk.MessagesDelta
		for _, m := range deltaMessage {
			if _, ok := index[m]; ok {
				continue
			}
			index[m] = struct{}{}
			state = append(state, m)
		}
		neighbors[source].lastSyncedVersion = syncOk.Delta
		return nil
	}

	n.Handle("sync", func(msg maelstrom.Message) error {
		lock.Lock()
		defer lock.Unlock()
		var sync Sync
		if err := json.Unmarshal(msg.Body, &sync); err != nil {
			return err
		}
		delta, messagesDelta, err := calculateDelta(sync.Delta)
		if err != nil {
			return err
		}
		return n.Reply(msg, SyncOk{Type: "sync_ok", Delta: delta, MessagesDelta: messagesDelta})
	})
	n.Handle("sync_ok", func(msg maelstrom.Message) error {
		return handleSyncOk(msg.Src, msg)
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var broadcast Broadcast
		if err := json.Unmarshal(msg.Body, &broadcast); err != nil {
			return err
		}

		message := broadcast.Message
		ok := BroadcastOk{Type: "broadcast_ok"}

		lock.Lock()
		defer lock.Unlock()
		if _, exists := index[message]; exists {
			return n.Reply(msg, ok)
		}
		index[message] = struct{}{}
		state = append(state, message)

		for _, neighbor := range neighbors {
			if strings.EqualFold(neighbor.id, msg.Src) {
				continue
			}
			//TODO: Batch each 50ms the Broadcast
			// n.Send(neighbor.id, broadcast)
		}

		return n.Reply(msg, ok)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		var topology Topology
		if err := json.Unmarshal(msg.Body, &topology); err != nil {
			return err
		}

		messageNeighbors := topology.Topology[n.ID()]

		for _, nnb := range messageNeighbors {
			neigh := &Neighbor{id: nnb, lastSyncedVersion: 0}
			neighbors[nnb] = neigh
		}

		return n.Reply(msg, TopologyOk{Type: "topology_ok"})
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		var read Read
		if err := json.Unmarshal(msg.Body, &read); err != nil {
			return err
		}
		return n.Reply(msg, ReadOk{Type: "read_ok", Messages: state})
	})

	bg := context.Background()

	isNeighborsReady := func() bool {
		return len(neighbors) != 0
	}
	// This is a hard NO
	//TODO: maybe move into different WG to control 2 concurrent syncs
	go func() {
		for range time.Tick(time.Millisecond * 250) {
			if !isNeighborsReady() {
				continue
			}
			for _, nb := range neighbors {
				ctx, cancel := context.WithTimeout(bg, time.Second*10)
				defer cancel()
				msg, err := n.SyncRPC(ctx, nb.id, Sync{Type: "sync", Delta: nb.lastSyncedVersion})
				if err != nil {
					continue
				}
				handleSyncOk(nb.id, msg)
			}
		}
	}()

	go func() {
		for range time.Tick(time.Millisecond * 125) {
			if !isNeighborsReady() {
				continue
			}
			for _, nb := range neighbors {
				sendBatch(nb.id, nb.lastSyncedVersion)
			}
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
