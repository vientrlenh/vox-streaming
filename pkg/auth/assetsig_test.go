package auth

import (
	"fmt"
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
