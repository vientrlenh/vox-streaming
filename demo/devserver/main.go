// Command devserver is a DEV-ONLY helper for the vox-streaming demo.
//
// It bundles the two pieces a browser client cannot provide on its own:
//
//  1. A stub of the exam gRPC service (ExamService). The streaming server calls
//     ValidateAccess before every WebRTC stream and UpdateRecording after the
//     recording is assembled; without something answering on
//     EXAM_SERVICE_GRPC_ADDR the server denies every stream. This stub always
//     allows access and accepts recordings.
//
//  2. A tiny HTTP endpoint that mints signed JWTs (student / teacher) using the
//     same JWT_STREAM_SECRET the server validates against. The browser must
//     never hold the secret, so token minting lives here.
//
// NEVER deploy this. It grants access to everyone and hands out tokens freely.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/vientrlenh/vox-streaming/pkg/auth"
	examv1 "github.com/vientrlenh/vox-streaming/pkg/pb/exam/v1"
	"google.golang.org/grpc"
)

// Fixed dev-only identifiers so a student token and a teacher/monitor token
// minted without explicit query params still point at the same schedule and
// can see each other in the monitor page. MonitorClaims requires userId/
// schoolId/examId/scheduleId to be valid UUIDs (see pkg/auth/jwt.go
// validateUUID); StreamClaims has no such constraint, so these are reused
// as-is for the student side too.
const (
	defaultScheduleID = "00000000-0000-0000-0000-000000000001"
	defaultExamID     = "00000000-0000-0000-0000-000000000002"
	defaultSchoolID   = "00000000-0000-0000-0000-000000000003"
	defaultTeacherID  = "00000000-0000-0000-0000-000000000004"
)

// examStub implements examv1.ExamServiceServer and permits everything.
type examStub struct {
	examv1.UnimplementedExamServiceServer
}

func (examStub) ValidateAccess(_ context.Context, req *examv1.ValidateAccessRequest) (*examv1.ValidateAccessResponse, error) {
	log.Printf("[exam-stub] ValidateAccess schedule=%s candidate=%s streamType=%s -> allowed",
		req.GetScheduleId(), req.GetCandidateId(), req.GetStreamType())
	return &examv1.ValidateAccessResponse{Allowed: true, Reason: "demo: always allowed"}, nil
}

func (examStub) UpdateRecording(_ context.Context, req *examv1.UpdateRecordingRequest) (*examv1.UpdateRecordingResponse, error) {
	log.Printf("[exam-stub] UpdateRecording stream=%s schedule=%s url=%s durationSecs=%d -> ok",
		req.GetStreamId(), req.GetScheduleId(), req.GetRecordingUrl(), req.GetDurationSecs())
	return &examv1.UpdateRecordingResponse{Success: true}, nil
}

func main() {
	secret := os.Getenv("JWT_STREAM_SECRET")
	if secret == "" {
		log.Fatalf("JWT_STREAM_SECRET not found in environment")
	}
	grpcAddr := envOr("GRPC_LISTEN", ":9095")
	httpAddr := envOr("HTTP_LISTEN", ":8090")

	// gRPC exam stub
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen %s: %v", grpcAddr, err)
	}
	gs := grpc.NewServer()
	examv1.RegisterExamServiceServer(gs, examStub{})
	go func() {
		log.Printf("[exam-stub] gRPC ExamService listening on %s", grpcAddr)
		if err := gs.Serve(lis); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	// HTTP token minter
	mux := http.NewServeMux()
	mux.HandleFunc("/token", tokenHandler([]byte(secret)))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	log.Printf("[token] HTTP token minter listening on %s (GET /token?role=student&scheduleId=%s&sessionId=session-1)", httpAddr, defaultScheduleID)
	if err := http.ListenAndServe(httpAddr, mux); err != nil {
		log.Fatalf("http serve: %v", err)
	}
}

// tokenHandler mints a StreamClaims (student) or MonitorClaims (teacher) JWT
// signed with the shared secret.
//
//	GET /token?role=student&scheduleId=...&userId=alice&examId=...&sessionId=session-1&streamTypes=camera,screen&ttl=2h
//	GET /token?role=teacher&scheduleId=...&userId=<uuid>&schoolId=<uuid>&examId=...&monitorScope=SCHEDULE_PROCTOR&ttl=2h
//
// sessionId defaults to "<scheduleId>:<userId>" when omitted, so re-minting a
// token for the same schedule+user (e.g. simulating a reconnect) reuses the same
// session id instead of generating a fresh one each time.
func tokenHandler(secret []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Permissive CORS so the page also works if hit cross-origin (dev only).
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		q := r.URL.Query()
		role := strings.ToLower(defaultStr(q.Get("role"), "student"))
		scheduleID := defaultStr(q.Get("scheduleId"), defaultScheduleID)
		examID := defaultStr(q.Get("examId"), defaultExamID)

		ttl := 2 * time.Hour
		if v := q.Get("ttl"); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				ttl = d
			}
		}

		now := time.Now()
		var token *jwt.Token
		resp := map[string]any{"role": role, "scheduleId": scheduleID, "examId": examID}

		switch role {
		case "student":
			userID := defaultStr(q.Get("userId"), "student-1")
			sessionID := defaultStr(q.Get("sessionId"), scheduleID+":"+userID)
			streamTypes := splitCSV(defaultStr(q.Get("streamTypes"), "camera,screen"))

			claims := auth.StreamClaims{
				CandidateID: userID,
				SessionID:   sessionID,
				ScheduleID:  scheduleID,
				ExamID:      examID,
				StreamTypes: streamTypes,
				TokenUse:    auth.TokenUseStream,
				RegisteredClaims: jwt.RegisteredClaims{
					Subject:   userID,
					IssuedAt:  jwt.NewNumericDate(now),
					ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
				},
			}
			token = jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
			resp["userId"] = userID
			resp["sessionId"] = sessionID
			resp["streamTypes"] = streamTypes

		case "teacher", "monitor":
			role = "teacher"
			userID := defaultStr(q.Get("userId"), defaultTeacherID)
			schoolID := defaultStr(q.Get("schoolId"), defaultSchoolID)
			monitorScope := defaultStr(q.Get("monitorScope"), string(auth.MonitorScopeScheduleProctor))

			claims := auth.MonitorClaims{
				UserID:       userID,
				SchoolID:     schoolID,
				MonitorScope: monitorScope,
				ScheduleIDs:  []string{scheduleID},
				ExamID:       examID,
				Roles:        []string{string(auth.RoleTeacher)},
				TokenUse:     auth.TokenUseMonitor,
				RegisteredClaims: jwt.RegisteredClaims{
					ID:        uuid.NewString(),
					Subject:   userID,
					IssuedAt:  jwt.NewNumericDate(now),
					ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
				},
			}
			token = jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
			resp["userId"] = userID
			resp["schoolId"] = schoolID
			resp["monitorScope"] = monitorScope

		default:
			http.Error(w, "role must be student or teacher", http.StatusBadRequest)
			return
		}

		signed, err := token.SignedString(secret)
		if err != nil {
			http.Error(w, "sign token failed", http.StatusInternalServerError)
			return
		}
		resp["token"] = signed
		resp["expiresIn"] = ttl.String()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Printf("[token] encode response failed: %v", err)
			return
		}
		log.Printf("[token] minted role=%s schedule=%s ttl=%s", role, scheduleID, ttl)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func defaultStr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
