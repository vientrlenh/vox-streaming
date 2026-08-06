package auth

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func assetValidator(t *testing.T, secret string) *Validator {
	t.Helper()
	t.Setenv("JWT_STREAM_SECRET", secret)
	v, err := NewValidator()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestSignAssetRoundTrip(t *testing.T) {
	v := assetValidator(t, "asset-secret-asset-secret")
	sig := v.SignAsset("stream-1")

	if err := v.VerifyAsset("stream-1", sig); err != nil {
		t.Fatalf("freshly minted signature should verify: %v", err)
	}
	// Compactness is the whole reason this exists instead of repeating the JWT.
	if len(sig) > 40 {
		t.Errorf("signature got long (%d bytes): %q", len(sig), sig)
	}
}

func TestVerifyAssetRejectsOtherStream(t *testing.T) {
	v := assetValidator(t, "asset-secret-asset-secret")
	sig := v.SignAsset("stream-1")

	if err := v.VerifyAsset("stream-2", sig); err == nil {
		t.Fatal("a signature for one stream must not authorize another")
	}
}

func TestVerifyAssetRejectsOtherSecret(t *testing.T) {
	sig := assetValidator(t, "secret-one-secret-one").SignAsset("stream-1")

	if err := assetValidator(t, "secret-two-secret-two").VerifyAsset("stream-1", sig); err == nil {
		t.Fatal("a signature minted under a different secret must not verify")
	}
}

func TestVerifyAssetRejectsExpired(t *testing.T) {
	v := assetValidator(t, "asset-secret-asset-secret")
	expired := time.Now().Add(-time.Minute).Unix()
	sig := fmt.Sprintf("%d.%s", expired, v.assetMAC("stream-1", expired))

	err := v.VerifyAsset("stream-1", sig)
	if err == nil {
		t.Fatal("an expired signature must not verify")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("want an expiry error, got %v", err)
	}
}

// The expiry travels in the clear next to the MAC, so it has to be covered by it
// — otherwise anyone could extend their own capability indefinitely.
func TestVerifyAssetRejectsTamperedExpiry(t *testing.T) {
	v := assetValidator(t, "asset-secret-asset-secret")
	sig := v.SignAsset("stream-1")
	_, mac, _ := strings.Cut(sig, ".")
	forged := fmt.Sprintf("%d.%s", time.Now().Add(100*time.Hour).Unix(), mac)

	if err := v.VerifyAsset("stream-1", forged); err == nil {
		t.Fatal("pushing the expiry out must invalidate the MAC")
	}
}

func TestVerifyAssetRejectsMalformed(t *testing.T) {
	v := assetValidator(t, "asset-secret-asset-secret")
	for _, sig := range []string{"", ".", "nodot", "abc.def", "999999999999999999999999.x"} {
		if err := v.VerifyAsset("stream-1", sig); err == nil {
			t.Errorf("expected %q to be rejected", sig)
		}
	}
}

// The signature is embedded in the #EXT-X-MAP URI of every live-rewind playlist,
// and hls.js treats a changed URI as a different init segment: it refetches and
// re-appends it, resetting the decoder and stalling playback. A playlist is
// rebuilt on every poll, so a signature derived from the exact call time changed
// on every poll and stalled the viewer roughly once per fragment.
//
// Asserted structurally rather than by sleeping: exp must sit on a bucket
// boundary offset by the TTL, which pins the property regardless of when the
// test happens to run.
func TestSignAssetIsStableWithinABucket(t *testing.T) {
	v := assetValidator(t, "asset-secret-asset-secret")

	sig := v.SignAsset("stream-1")
	rawExp, _, ok := strings.Cut(sig, ".")
	if !ok {
		t.Fatalf("malformed signature %q", sig)
	}
	exp, err := strconv.ParseInt(rawExp, 10, 64)
	if err != nil {
		t.Fatalf("malformed expiry in %q: %v", sig, err)
	}

	bucketSecs := int64(assetSignatureBucket.Seconds())
	if (exp-int64(AssetSignatureTTL.Seconds()))%bucketSecs != 0 {
		t.Fatalf("expiry %d is not anchored to a %s bucket boundary; the signature will change on every playlist poll", exp, assetSignatureBucket)
	}

	// Repeated calls inside the same bucket must be byte-identical, which is what
	// keeps the playlist stable between polls.
	if again := v.SignAsset("stream-1"); again != sig {
		t.Fatalf("signature changed between calls: %q then %q", sig, again)
	}
}

// Bucketing shortens a signature's remaining life by at most one bucket. That
// residue still has to comfortably outlast the gap between a playlist being
// served and its fragments being fetched.
func TestSignAssetKeepsUsefulLifetimeAfterBucketing(t *testing.T) {
	v := assetValidator(t, "asset-secret-asset-secret")

	sig := v.SignAsset("stream-1")
	rawExp, _, _ := strings.Cut(sig, ".")
	exp, err := strconv.ParseInt(rawExp, 10, 64)
	if err != nil {
		t.Fatalf("malformed expiry in %q: %v", sig, err)
	}

	remaining := time.Duration(exp-time.Now().Unix()) * time.Second
	if floor := AssetSignatureTTL - assetSignatureBucket; remaining < floor {
		t.Fatalf("remaining lifetime %s dropped below the %s floor", remaining, floor)
	}
	if remaining > AssetSignatureTTL {
		t.Fatalf("remaining lifetime %s exceeds the declared TTL %s", remaining, AssetSignatureTTL)
	}
}
