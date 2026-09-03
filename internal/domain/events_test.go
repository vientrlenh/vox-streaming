package domain

import "testing"

func TestDefaultAlertLevel(t *testing.T) {
	tests := []struct {
		alertType string
		want      AlertLevel
	}{
		{AlertPhoneDetected, AlertLevelCritical},
		{AlertProhibitedObject, AlertLevelCritical},
		{AlertMultiplePersons, AlertLevelWarning},
		{AlertPersonMissing, AlertLevelWarning},
		{AlertUncooperativeCandidate, AlertLevelWarning},
		{AlertStreamDropped, AlertLevelWarning},
		{AlertRecordingIncomplete, AlertLevelWarning},
		{AlertWindowFocusLost, AlertLevelWarning},
		{AlertCameraSignalLost, AlertLevelWarning},
		// Bản ghi vẫn dùng được, chỉ đuôi cụt -- chỉ ghi vào sổ, không đẩy lên lưới giám sát.
		{AlertRecordingTruncated, AlertLevelInfo},
		// Sự cố đã qua lúc nó phát ra; nó chỉ đóng khoảng trong sổ cho người chấm.
		{AlertCameraSignalRestored, AlertLevelInfo},
		{"SOME_UNKNOWN_ALERT_TYPE", AlertLevelInfo},
		{"", AlertLevelInfo},
		// Hai tên vừa bỏ khỏi từ vựng vì không nơi nào phát chúng. Rơi về INFO là đúng: một message
		// mang tên này nghĩa là có bản cũ chưa deploy xong, không phải một phép phát hiện thật.
		{"TRACK_ENDED", AlertLevelInfo},
		{"RECONNECT_LOOP", AlertLevelInfo},
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
