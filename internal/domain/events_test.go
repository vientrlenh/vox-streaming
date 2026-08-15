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
		{AlertPersonMissing, AlertLevelWarning},
		{AlertUncooperativeCandidate, AlertLevelWarning},
		{AlertStreamDropped, AlertLevelWarning},
		{AlertTrackEnded, AlertLevelWarning},
		{AlertReconnectLoop, AlertLevelWarning},
		{AlertRecordingIncomplete, AlertLevelWarning},
		{AlertWindowFocusLost, AlertLevelWarning},
		{"SOME_UNKNOWN_ALERT_TYPE", AlertLevelInfo},
		{"", AlertLevelInfo},
		// Tên cũ của hai loại vừa đổi. Chúng RƠI VỀ INFO và điều đó là đúng: nguồn phát không còn
		// dùng chúng nữa, nên một message mang tên cũ nghĩa là có bản cũ chưa deploy xong. Bản ghi
		// lịch sử trong DB thì đã được migration V37 nâng mức, không phụ thuộc vào hàm này.
		{"OBJECT_DETECTED", AlertLevelInfo},
		{"CRITICAL_VIOLATION", AlertLevelInfo},
	}
	for _, tt := range tests {
		t.Run(tt.alertType, func(t *testing.T) {
			if got := DefaultAlertLevel(tt.alertType); got != tt.want {
				t.Errorf("DefaultAlertLevel(%q) = %v, want %v", tt.alertType, got, tt.want)
			}
		})
	}
}
