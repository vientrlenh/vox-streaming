package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// DeclaredSegment is one segment the client says it captured, whether or not it managed to upload
// it. FramesWritten is what separates "nothing happened on screen" from "capture froze": both
// produce a small segment, only the frame count tells them apart.
type DeclaredSegment struct {
	Seq           int64     `json:"seq"`
	StartedAt     time.Time `json:"startedAt"`
	EndedAt       time.Time `json:"endedAt"`
	SHA256        string    `json:"sha256"`
	SizeBytes     int64     `json:"sizeBytes"`
	FramesWritten int64     `json:"framesWritten"`
}

// StreamInventory is the client's own account of what a stream contains.
//
// It exists because the server otherwise has no way to tell "the client uploaded everything it
// had" from "the client died with segments still on disk": both look identical from here -- a set
// of segments that simply stops. Comparing what arrived against what the client declared turns gap
// detection from an inference into a subtraction, and unlike reconciling against the parallel
// WebRTC recording it keeps working when the network was the thing that failed, which is precisely
// the case that matters.
//
// Complete reports whether the client believes it has finished producing segments; until then a
// missing tail is expected rather than a gap.
type StreamInventory struct {
	StreamID   string            `json:"streamId"`
	Complete   bool              `json:"complete"`
	DeclaredAt time.Time         `json:"declaredAt"`
	Segments   []DeclaredSegment `json:"segments"`
}

type InventoryRegistry struct {
	client *redis.Client
}

func NewInventoryRegistry(client *redis.Client) *InventoryRegistry {
	return &InventoryRegistry{client: client}
}

// Put replaces the stream's inventory wholesale. Whole-document rather than incremental on purpose:
// the client always knows its complete set, a full replacement cannot drift out of sync the way a
// series of deltas can, and a late-arriving stale copy is harmless because inventories only ever
// grow -- see the DeclaredAt guard.
func (r *InventoryRegistry) Put(ctx context.Context, inventory StreamInventory) error {
	existing, err := r.Get(ctx, inventory.StreamID)
	if err != nil {
		return err
	}
	// Two uploads racing (a periodic declaration overtaken by the one sent before /complete) must
	// not let the older, shorter list win.
	if existing != nil && existing.DeclaredAt.After(inventory.DeclaredAt) {
		return nil
	}

	data, err := json.Marshal(inventory)
	if err != nil {
		return fmt.Errorf("marshal inventory: %w", err)
	}
	if err := r.client.Set(ctx, inventoryKey(inventory.StreamID), string(data), segmentTTL).Err(); err != nil {
		return fmt.Errorf("store inventory: %w", err)
	}
	return nil
}

// Get returns nil (and no error) when the client never declared an inventory for this stream --
// an older client, or one that died before its first declaration.
func (r *InventoryRegistry) Get(ctx context.Context, streamID string) (*StreamInventory, error) {
	raw, err := r.client.Get(ctx, inventoryKey(streamID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load inventory: %w", err)
	}

	var inventory StreamInventory
	if err := json.Unmarshal(raw, &inventory); err != nil {
		return nil, fmt.Errorf("unmarshal inventory: %w", err)
	}
	return &inventory, nil
}

func inventoryKey(streamID string) string {
	return fmt.Sprintf("stream:%s:inventory", streamID)
}
