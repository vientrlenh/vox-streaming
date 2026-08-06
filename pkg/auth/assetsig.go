package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Live-rewind HLS asset signatures.
//
// A DVR playlist for a multi-hour exam is thousands of lines, and every line has
// to carry its own credential: a playlist URI does not inherit the playlist's
// query string, and native HLS players (Safari) offer no request hook to add one.
// Repeating the monitor JWT per line costs ~450 bytes each — over a megabyte of
// string built per poll, every few seconds, per watching teacher. This is the
// same signed-URL pattern the presigned S3 URLs it replaced used, just compact
// (~60 bytes) and scoped to exactly one stream.
//
// It is a read capability for one stream's already-recorded fragments, minted
// only after a full monitor-token check on the playlist request, and it expires.
// It is NOT a substitute for that check anywhere else.

// assetSigBytes truncates the HMAC to 128 bits. Full SHA-256 would double the
// per-line cost for no meaningful gain against a secret-keyed MAC on a
// short-lived, single-stream read capability.
const assetSigBytes = 16

// AssetSignatureTTL is how long a minted signature stays valid. Comfortably
// longer than the few seconds between a playlist being served and its fragments
// being fetched, while still bounding replay if a playlist URL leaks.
const AssetSignatureTTL = 30 * time.Minute

// assetSignatureBucket is the window over which SignAsset returns a byte-identical
// string.
//
// The signature is part of the #EXT-X-MAP URI in every live-rewind playlist, and
// hls.js identifies the init segment BY ITS URI: a changed URI is a different init
// segment, so it refetches and re-appends it, which resets the decoder and shows
// the viewer a buffering stall. Deriving exp straight from time.Now() changed the
// signature every second, so every playlist poll (~once per target duration)
// produced a fresh URI and a fresh stall — the exact buffer churn this file's
// design was supposed to have eliminated when it replaced presigned S3 URLs.
//
// Quantising exp to a bucket boundary makes the whole playlist stable between
// polls. The cost is only that a signature's remaining lifetime varies within
// [TTL-bucket, TTL] instead of being exactly TTL; keep bucket well under TTL so
// even a signature minted at the end of a bucket still outlives any reasonable
// fetch delay.
const assetSignatureBucket = 10 * time.Minute

// SignAsset returns "<expUnix>.<sig>" authorizing reads of streamID's
// live-rewind assets. The expiry is anchored to the current bucket boundary
// rather than to the exact call time, so repeated calls within a bucket return
// the same string — see assetSignatureBucket for why that matters.
func (v *Validator) SignAsset(streamID string) string {
	exp := time.Now().Truncate(assetSignatureBucket).Add(AssetSignatureTTL).Unix()
	return fmt.Sprintf("%d.%s", exp, v.assetMAC(streamID, exp))
}

// VerifyAsset checks a signature produced by SignAsset against streamID.
func (v *Validator) VerifyAsset(streamID, signature string) error {
	rawExp, mac, ok := strings.Cut(signature, ".")
	if !ok {
		return errors.New("malformed asset signature")
	}
	exp, err := strconv.ParseInt(rawExp, 10, 64)
	if err != nil {
		return errors.New("malformed asset signature expiry")
	}
	// Compare before checking expiry so a wrong key and an expired key take the
	// same path length.
	if !hmac.Equal([]byte(mac), []byte(v.assetMAC(streamID, exp))) {
		return errors.New("invalid asset signature")
	}
	if time.Now().Unix() > exp {
		return errors.New("asset signature expired")
	}
	return nil
}

func (v *Validator) assetMAC(streamID string, exp int64) string {
	mac := hmac.New(sha256.New, v.secret)
	// Length-prefixed so no (streamID, exp) pair can be confused with another by
	// shifting bytes across the separator.
	fmt.Fprintf(mac, "live-asset\x00%d\x00%s\x00%d", len(streamID), streamID, exp)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)[:assetSigBytes])
}
