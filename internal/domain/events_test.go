package domain

import "testing"

func TestDefaultAlertLevel(t *testing.T) {
	tests := []struct {
		alertType string
		want      AlertLevel
	}{
		{AlertPhoneDetected, AlertLevelCritical},
		{AlertMultiplePersons, AlertLevelCritical},
		{AlertProhibitedObject, AlertLevelCritical},
		{AlertFaceNotVisible, AlertLevelWarning},
		{AlertSuspiciousGaze, AlertLevelWarning},
		{AlertStreamDropped, AlertLevelWarning},
		{AlertTrackEnded, AlertLevelWarning},
		{AlertReconnectLoop, AlertLevelWarning},
		{AlertRecordingIncomplete, AlertLevelWarning},
		{"SOME_UNKNOWN_ALERT_TYPE", AlertLevelInfo},
		{"", AlertLevelInfo},
	}
	for _, tt := range tests {
		t.Run(tt.alertType, func(t *testing.T) {
			if got := DefaultAlertLevel(tt.alertType); got != tt.want {
				t.Errorf("DefaultAlertLevel(%q) = %v, want %v", tt.alertType, got, tt.want)
			}
		})
	}
}
