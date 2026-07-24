package auth

import (
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const testSecret = "test-secret-do-not-use-in-prod"

func signClaims(t *testing.T, secret string, claims jwt.Claims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign claims: %v", err)
	}
	return signed
}

func validStreamClaims() StreamClaims {
	return StreamClaims{
		CandidateID: uuid.NewString(),
		SessionID:   uuid.NewString(),
		ScheduleID:  uuid.NewString(),
		ExamID:      uuid.NewString(),
		StreamTypes: []string{"camera", "screen"},
		TokenUse:    TokenUseStream,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
}

func TestValidateStream(t *testing.T) {
	v := &Validator{secret: []byte(testSecret)}

	t.Run("valid token succeeds", func(t *testing.T) {
		claims := validStreamClaims()
		got, err := v.ValidateStream(signClaims(t, testSecret, claims))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.CandidateID != claims.CandidateID || got.ScheduleID != claims.ScheduleID {
			t.Errorf("got %+v, want candidateId=%s scheduleId=%s", got, claims.CandidateID, claims.ScheduleID)
		}
	})

	t.Run("missing token rejected", func(t *testing.T) {
		if _, err := v.ValidateStream(""); err == nil {
			t.Fatal("expected error for empty token")
		}
	})

	t.Run("expired token rejected", func(t *testing.T) {
		claims := validStreamClaims()
		claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Hour))
		_, err := v.ValidateStream(signClaims(t, testSecret, claims))
		if !errors.Is(err, jwt.ErrTokenExpired) {
			t.Fatalf("got %v, want wrapped jwt.ErrTokenExpired", err)
		}
	})

	t.Run("wrong signing secret rejected", func(t *testing.T) {
		claims := validStreamClaims()
		token := signClaims(t, "a-completely-different-secret", claims)
		_, err := v.ValidateStream(token)
		if !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
			t.Fatalf("got %v, want wrapped jwt.ErrTokenSignatureInvalid", err)
		}
	})

	t.Run("alg=none token rejected", func(t *testing.T) {
		claims := validStreamClaims()
		token := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
		signed, err := token.SignedString(jwt.UnsafeAllowNoneSignatureType)
		if err != nil {
			t.Fatalf("sign none token: %v", err)
		}
		if _, err := v.ValidateStream(signed); err == nil {
			t.Fatal("expected error for alg=none token")
		}
	})

	tests := []struct {
		name    string
		mutate  func(c *StreamClaims)
		wantErr string
	}{
		{"wrong token use", func(c *StreamClaims) { c.TokenUse = TokenUseMonitor }, "invalid token use"},
		{"missing candidateId", func(c *StreamClaims) { c.CandidateID = "" }, "missing candidateId"},
		{"missing sessionId", func(c *StreamClaims) { c.SessionID = "" }, "missing sessionId"},
		{"missing scheduleId", func(c *StreamClaims) { c.ScheduleID = "" }, "missing scheduleId"},
		{"missing examId", func(c *StreamClaims) { c.ExamID = "" }, "missing examId"},
		{"missing streamTypes", func(c *StreamClaims) { c.StreamTypes = nil }, "missing streamTypes"},
		{"unsupported stream type", func(c *StreamClaims) { c.StreamTypes = []string{"microphone"} }, "unsupported stream type: microphone"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claims := validStreamClaims()
			tt.mutate(&claims)
			_, err := v.ValidateStream(signClaims(t, testSecret, claims))
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("got %v, want error %q", err, tt.wantErr)
			}
		})
	}
}

func validMonitorClaims() MonitorClaims {
	userID := uuid.NewString()
	return MonitorClaims{
		UserID:       userID,
		SchoolID:     uuid.NewString(),
		MonitorScope: string(MonitorScopeSchoolAdmin),
		ScheduleIDs:  []string{uuid.NewString(), uuid.NewString()},
		ExamID:       uuid.NewString(),
		Roles:        []string{string(RoleSchoolAdmin)},
		TokenUse:     TokenUseMonitor,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			ID:        uuid.NewString(),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
}

func TestValidateMonitor(t *testing.T) {
	v := &Validator{secret: []byte(testSecret)}

	t.Run("valid token succeeds", func(t *testing.T) {
		claims := validMonitorClaims()
		got, err := v.ValidateMonitor(signClaims(t, testSecret, claims))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.UserID != claims.UserID || len(got.ScheduleIDs) != 2 {
			t.Errorf("got %+v, want userId=%s with 2 scheduleIds", got, claims.UserID)
		}
	})

	tests := []struct {
		name    string
		mutate  func(c *MonitorClaims)
		wantErr string
	}{
		{"wrong token use", func(c *MonitorClaims) { c.TokenUse = TokenUseStream }, "invalid token use"},
		{"missing userId", func(c *MonitorClaims) { c.UserID = ""; c.Subject = "" }, "missing userId"},
		{"subject/userId mismatch", func(c *MonitorClaims) { c.Subject = uuid.NewString() }, "subject and userId mismatch"},
		{"missing schoolId", func(c *MonitorClaims) { c.SchoolID = "" }, "missing schoolId"},
		{"missing examId", func(c *MonitorClaims) { c.ExamID = "" }, "missing examId"},
		{"missing scheduleIds", func(c *MonitorClaims) { c.ScheduleIDs = nil }, "missing scheduleIds"},
		{"missing jti", func(c *MonitorClaims) { c.ID = "" }, "missing jti"},
		{"missing monitor role", func(c *MonitorClaims) { c.Roles = []string{"STUDENT"} }, "missing monitor role"},
		{"invalid userId uuid", func(c *MonitorClaims) { c.UserID = "not-a-uuid"; c.Subject = "not-a-uuid" }, "invalid userId"},
		{"invalid schoolId uuid", func(c *MonitorClaims) { c.SchoolID = "not-a-uuid" }, "invalid schoolId"},
		{"invalid examId uuid", func(c *MonitorClaims) { c.ExamID = "not-a-uuid" }, "invalid examId"},
		{"invalid scheduleId uuid", func(c *MonitorClaims) { c.ScheduleIDs = []string{"not-a-uuid"} }, "invalid scheduleId"},
		{"duplicate scheduleId", func(c *MonitorClaims) {
			id := uuid.NewString()
			c.ScheduleIDs = []string{id, id}
		}, "duplicate scheduleId"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claims := validMonitorClaims()
			tt.mutate(&claims)
			_, err := v.ValidateMonitor(signClaims(t, testSecret, claims))
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("got %v, want error %q", err, tt.wantErr)
			}
		})
	}
}

func TestStreamClaims_CanStream(t *testing.T) {
	c := &StreamClaims{StreamTypes: []string{"camera"}}
	if !c.CanStream("camera") {
		t.Error("expected camera to be allowed")
	}
	if c.CanStream("screen") {
		t.Error("expected screen to be rejected")
	}
}

func TestStreamClaims_CanAccess(t *testing.T) {
	scheduleID := uuid.NewString()
	c := &StreamClaims{ScheduleID: scheduleID, StreamTypes: []string{"camera"}}

	if !c.CanAccess(scheduleID, "camera") {
		t.Error("expected access to be allowed for matching schedule + stream type")
	}
	if c.CanAccess(uuid.NewString(), "camera") {
		t.Error("expected access to be denied for a different scheduleId")
	}
	if c.CanAccess(scheduleID, "screen") {
		t.Error("expected access to be denied for a disallowed stream type")
	}
}

func TestMonitorClaims_HasValidScopeRole(t *testing.T) {
	tests := []struct {
		name  string
		scope MonitorScope
		roles []string
		want  bool
	}{
		{"school admin scope with school admin role", MonitorScopeSchoolAdmin, []string{string(RoleSchoolAdmin)}, true},
		{"school admin scope with teacher role", MonitorScopeSchoolAdmin, []string{string(RoleTeacher)}, false},
		{"exam chair scope with teacher role", MonitorScopeExamChair, []string{string(RoleTeacher)}, true},
		{"exam chair scope with school admin role", MonitorScopeExamChair, []string{string(RoleSchoolAdmin)}, false},
		{"schedule proctor scope with teacher role", MonitorScopeScheduleProctor, []string{string(RoleTeacher)}, true},
		{"unknown scope", MonitorScope("UNKNOWN"), []string{string(RoleTeacher), string(RoleSchoolAdmin)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &MonitorClaims{MonitorScope: string(tt.scope), Roles: tt.roles}
			if got := c.HasValidScopeRole(); got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMonitorClaims_CanMonitorSchedule(t *testing.T) {
	scheduleID := uuid.NewString()
	c := &MonitorClaims{
		MonitorScope: string(MonitorScopeSchoolAdmin),
		Roles:        []string{string(RoleSchoolAdmin)},
		ScheduleIDs:  []string{scheduleID},
	}

	if !c.CanMonitorSchedule(scheduleID) {
		t.Error("expected schedule in ScheduleIDs to be allowed")
	}
	if c.CanMonitorSchedule(uuid.NewString()) {
		t.Error("expected a schedule outside ScheduleIDs to be denied")
	}

	c.Roles = []string{"STUDENT"}
	if c.CanMonitorSchedule(scheduleID) {
		t.Error("expected an invalid scope role to be denied even if schedule matches")
	}
}
