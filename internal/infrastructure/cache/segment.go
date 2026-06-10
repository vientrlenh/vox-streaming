package cache

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const segmentTTL = 24 * time.Hour

type SegmentMeta struct {
	Seq        int64     `json:"seq"`
	S3Key      string    `json:"s3_key"`
	StartedAt  time.Time `json:"started_at"`
	EndedAt    time.Time `json:"ended_at"`
	SizeBytes  int64     `json:"size_bytes"`
	UploadedAt time.Time `json:"uploaded_at"`
}

type SegmentRegistry struct {
	client *redis.Client
}

func NewSegmentRegistry(client *redis.Client) *SegmentRegistry {
	return &SegmentRegistry{
		client: client,
	}
}

func (r *SegmentRegistry) Add(ctx context.Context, streamID string, meta SegmentMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	key := segmentKey(streamID)
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, strconv.FormatInt(meta.Seq, 10), string(data))
	pipe.Expire(ctx, key, segmentTTL)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *SegmentRegistry) List(ctx context.Context, streamID string) ([]SegmentMeta, error) {
	result, err := r.client.HGetAll(ctx, segmentKey(streamID)).Result()
	if err != nil {
		return nil, err
	}

	metas := make([]SegmentMeta, 0, len(result))
	for _, v := range result {
		var meta SegmentMeta
		if err := json.Unmarshal([]byte(v), &meta); err != nil {
			continue
		}
		metas = append(metas, meta)
	}
	// HGEALL does not guarantee ordering, need sort by seq
	slices.SortFunc(metas, func(a, b SegmentMeta) int {
		return cmp.Compare(a.Seq, b.Seq)
	})
	return metas, nil
}

func segmentKey(streamID string) string {
	return fmt.Sprintf("stream:%s:segments", streamID)
}
