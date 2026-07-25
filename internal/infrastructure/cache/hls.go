package cache

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const hlsFragmentTTL = 24 * time.Hour // mirrors segmentTTL — must safely exceed any single exam's duration

// HLSInitMeta is the fMP4 initialization segment for one recorder attempt
// (Epoch). A stream restarts ffmpeg on crash (see RecorderSupervisor), and
// each attempt gets its own init segment.
type HLSInitMeta struct {
	Epoch      int       `json:"epoch"`
	S3Key      string    `json:"s3Key"`
	UploadedAt time.Time `json:"uploadedAt"`
}

// HLSFragmentMeta is one uploaded HLS media fragment (.m4s).
type HLSFragmentMeta struct {
	Seq        int64     `json:"seq"` // monotonic across the whole peer lifetime, not reset per attempt
	Epoch      int       `json:"epoch"`
	S3Key      string    `json:"s3Key"`
	StartedAt  time.Time `json:"startedAt"`
	EndedAt    time.Time `json:"endedAt"`
	SizeBytes  int64     `json:"sizeBytes"`
	UploadedAt time.Time `json:"uploadedAt"`
}

// HLSFragmentRegistry tracks the live HLS asset list for in-progress streams,
// written synchronously as each asset uploads (unlike SegmentRegistry, which
// the archival pipeline only flushes once a stream ends — this registry's
// entire purpose is availability DURING the live stream). It is separate from
// SegmentRegistry/SegmentMeta because that type is modeled around the
// archival/SHA256/desktop-upload use case and shouldn't be overloaded with
// unrelated HLS fields.
type HLSFragmentRegistry struct {
	client *redis.Client
}

func NewHLSFragmentRegistry(client *redis.Client) *HLSFragmentRegistry {
	return &HLSFragmentRegistry{client: client}
}

func (r *HLSFragmentRegistry) AddInit(ctx context.Context, streamID string, meta HLSInitMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	key := hlsInitRegistryKey(streamID)
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, strconv.Itoa(meta.Epoch), string(data))
	pipe.Expire(ctx, key, hlsFragmentTTL)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *HLSFragmentRegistry) AddFragment(ctx context.Context, streamID string, meta HLSFragmentMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	key := hlsFragmentRegistryKey(streamID)
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, strconv.FormatInt(meta.Seq, 10), string(data))
	pipe.Expire(ctx, key, hlsFragmentTTL)
	_, err = pipe.Exec(ctx)
	return err
}

// GetInit looks up a single epoch's init segment. Used on the asset-serving
// path (one HTTP request per fetched init segment), where HGET is O(1) —
// unlike ListInits/ListFragments, which HGETALL the whole registry and are
// only appropriate when building a manifest.
func (r *HLSFragmentRegistry) GetInit(ctx context.Context, streamID string, epoch int) (*HLSInitMeta, error) {
	val, err := r.client.HGet(ctx, hlsInitRegistryKey(streamID), strconv.Itoa(epoch)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var meta HLSInitMeta
	if err := json.Unmarshal(val, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// GetFragment looks up a single media fragment by its monotonic Seq. Returns
// (nil, nil) when that fragment was never registered (or has aged out).
func (r *HLSFragmentRegistry) GetFragment(ctx context.Context, streamID string, seq int64) (*HLSFragmentMeta, error) {
	val, err := r.client.HGet(ctx, hlsFragmentRegistryKey(streamID), strconv.FormatInt(seq, 10)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var meta HLSFragmentMeta
	if err := json.Unmarshal(val, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (r *HLSFragmentRegistry) ListInits(ctx context.Context, streamID string) ([]HLSInitMeta, error) {
	result, err := r.client.HGetAll(ctx, hlsInitRegistryKey(streamID)).Result()
	if err != nil {
		return nil, err
	}
	inits := make([]HLSInitMeta, 0, len(result))
	for _, v := range result {
		var meta HLSInitMeta
		if err := json.Unmarshal([]byte(v), &meta); err != nil {
			continue
		}
		inits = append(inits, meta)
	}
	slices.SortFunc(inits, func(a, b HLSInitMeta) int {
		return cmp.Compare(a.Epoch, b.Epoch)
	})
	return inits, nil
}

func (r *HLSFragmentRegistry) ListFragments(ctx context.Context, streamID string) ([]HLSFragmentMeta, error) {
	result, err := r.client.HGetAll(ctx, hlsFragmentRegistryKey(streamID)).Result()
	if err != nil {
		return nil, err
	}
	frags := make([]HLSFragmentMeta, 0, len(result))
	for _, v := range result {
		var meta HLSFragmentMeta
		if err := json.Unmarshal([]byte(v), &meta); err != nil {
			continue
		}
		frags = append(frags, meta)
	}
	// HGETALL does not guarantee ordering, need sort by seq
	slices.SortFunc(frags, func(a, b HLSFragmentMeta) int {
		return cmp.Compare(a.Seq, b.Seq)
	})
	return frags, nil
}

func hlsInitRegistryKey(streamID string) string {
	return fmt.Sprintf("stream:%s:hls-inits", streamID)
}

func hlsFragmentRegistryKey(streamID string) string {
	return fmt.Sprintf("stream:%s:hls-fragments", streamID)
}
