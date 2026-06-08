package auth

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/golang-jwt/jwt/v5"
)

type Role string

const (
	RoleStudent     Role = "STUDENT"
	RoleTeacher     Role = "TEACHER"
	RoleSchoolAdmin Role = "SCHOOL_ADMIN"
	RoleSystemAdmin Role = "SYSTEM_ADMIN"
)

type StreamClaims struct {
	UserID      string   `json:"userId"`
	RoomIDs     []string `json:"roomIds"`
	ExamID      string   `json:"examId"`
	Roles       []string `json:"roles"`
	StreamTypes []string `json:"streamTypes,omitempty"`
	jwt.RegisteredClaims
}

func (c *StreamClaims) CanStream(streamType string) bool {
	return slices.Contains(c.StreamTypes, streamType)
}

func (c *StreamClaims) CanMonitorRoom(roomID string) bool {
	if !c.hasMonitorRole() {
		return false
	}
	return slices.Contains(c.RoomIDs, roomID)
}

func (c *StreamClaims) IsStudent() bool {
	return c.hasRole(RoleStudent)
}

func (c *StreamClaims) hasMonitorRole() bool {
	return c.hasRole(RoleTeacher) || c.hasRole(RoleSchoolAdmin)
}

func (c *StreamClaims) hasRole(target Role) bool {
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

func (v *Validator) Validate(tokenStr string) (*StreamClaims, error) {
	if tokenStr == "" {
		return nil, errors.New("missing token")
	}
	token, err := jwt.ParseWithClaims(
		tokenStr,
		&StreamClaims{},
		func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return v.secret, nil
		},
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}

	claims, ok := token.Claims.(*StreamClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims")
	}
	return claims, nil
}
