package auth

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

type Role string

const (
	RoleTeacher     Role = "TEACHER"
	RoleSchoolAdmin Role = "SCHOOL_ADMIN"
)

const (
	TokenUseStream  = "stream"
	TokenUseMonitor = "monitor"
)

type MonitorScope string

const (
	MonitorScopeSchoolAdmin     MonitorScope = "SCHOOL_ADMIN"
	MonitorScopeExamChair       MonitorScope = "EXAM_CHAIR"
	MonitorScopeScheduleProctor MonitorScope = "SCHEDULE_PROCTOR"
)

type StreamClaims struct {
	CandidateID string   `json:"candidateId"`
	SessionID   string   `json:"sessionId"`
	ScheduleID  string   `json:"scheduleId"`
	ExamID      string   `json:"examId"`
	StreamTypes []string `json:"streamTypes"`
	TokenUse    string   `json:"tokenUse"`
	jwt.RegisteredClaims
}

type MonitorClaims struct {
	UserID       string   `json:"userId"`
	SchoolID     string   `json:"schoolId"`
	MonitorScope string   `json:"monitorScope"`
	ScheduleIDs  []string `json:"scheduleIds"`
	ExamID       string   `json:"examId"`
	Roles        []string `json:"roles"`
	TokenUse     string   `json:"tokenUse"`
	jwt.RegisteredClaims
}

func (c *StreamClaims) CanStream(streamType string) bool {
	return slices.Contains(c.StreamTypes, streamType)
}

func (c *StreamClaims) CanAccess(scheduleID, streamType string) bool {
	return c.ScheduleID == scheduleID && c.CanStream(streamType)
}

func (c *MonitorClaims) HasMonitorRole() bool {
	return c.hasRole(RoleTeacher) || c.hasRole(RoleSchoolAdmin)
}

func (c *MonitorClaims) HasValidScopeRole() bool {
	switch MonitorScope(c.MonitorScope) {
	case MonitorScopeSchoolAdmin: 
		return c.hasRole(RoleSchoolAdmin)
	case MonitorScopeExamChair, MonitorScopeScheduleProctor: 
		return c.hasRole(RoleTeacher)
	default: 
		return false
	}
}

func (c *MonitorClaims) CanMonitorSchedule(scheduleID string) bool {
	if !c.HasValidScopeRole() {
		return false
	}
	return slices.Contains(c.ScheduleIDs, scheduleID)
}

func (c *MonitorClaims) hasRole(target Role) bool {
	for _, r := range c.Roles {
		if Role(r) == target {
			return true
		}
	}
	return false
}

type Validator struct {
	secret []byte
}

func NewValidator() (*Validator, error) {
	secret := os.Getenv("JWT_STREAM_SECRET")
	if secret == "" {
		return nil, errors.New("JWT_STREAM_SECRET is not set")
	}
	return &Validator{
		secret: []byte(secret),
	}, nil
}

func (v *Validator) ValidateStream(tokenStr string) (*StreamClaims, error) {
	claims := &StreamClaims{}

	if err := v.parse(tokenStr, claims); err != nil {
		return nil, err
	}

	if claims.TokenUse != TokenUseStream {
		return nil, errors.New("invalid token use")
	}

	if claims.CandidateID == "" {
		return nil, errors.New("missing candidateId")
	}
	if claims.SessionID == "" {
		return nil, errors.New("missing sessionId")
	}
	if claims.ScheduleID == "" {
		return nil, errors.New("missing scheduleId")
	}
	if claims.ExamID == "" {
		return nil, errors.New("missing examId")
	}
	if len(claims.StreamTypes) == 0 {
		return nil, errors.New("missing streamTypes")
	}

	for _, streamType := range claims.StreamTypes {
		if streamType != "camera" && streamType != "screen" {
			return nil, fmt.Errorf("unsupported stream type: %s", streamType)
		}
	}

	return claims, nil
}

func (v *Validator) ValidateMonitor(tokenStr string) (*MonitorClaims, error) {
	claims := &MonitorClaims{}

	if err := v.parse(tokenStr, claims); err != nil {
		return nil, err
	}

	if claims.TokenUse != TokenUseMonitor {
		return nil, errors.New("invalid token use")
	}

	if claims.UserID == "" {
		return nil, errors.New("missing userId")
	}

	if claims.Subject != claims.UserID {
		return nil, errors.New("subject and userId mismatch")
	}

	if claims.SchoolID == "" {
		return nil, errors.New("missing schoolId")
	}

	if claims.ExamID == "" {
		return nil, errors.New("missing examId")
	}

	if len(claims.ScheduleIDs) == 0 {
		return nil, errors.New("missing scheduleIds")
	}

	if claims.ID == "" {
		return nil, errors.New("missing jti")
	}

	if !claims.HasMonitorRole() {
		return nil, errors.New("missing monitor role")
	}

	if err := validateUUID("userId", claims.UserID); err != nil {
		return nil, err
	}

	if err := validateUUID("schoolId", claims.SchoolID); err != nil {
		return nil, err
	}

	if err := validateUUID("examId", claims.ExamID); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(claims.ScheduleIDs)) 

	for _, scheduleID := range claims.ScheduleIDs {
		if err := validateUUID("scheduleId", scheduleID); err != nil {
			return nil, err
		}

		if _, exists := seen[scheduleID]; exists {
			return nil, errors.New("duplicate scheduleId")
		}

		seen[scheduleID] = struct{}{}
	}

	return claims, nil
}

func (v *Validator) parse(tokenStr string, claims jwt.Claims) error {
	if tokenStr == "" {
		return errors.New("missing token")
	}

	token, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return v.secret, nil
	}, jwt.WithExpirationRequired())
	if err != nil {
		return fmt.Errorf("parse token: %w", err)
	}

	if !token.Valid {
		return errors.New("invalid token")
	}

	return nil
}

func validateUUID(name , value string) error {
	if _, err := uuid.Parse(value); err != nil {
		return fmt.Errorf("invalid %s", name)
	}
	return nil
}