package server

import (
	"context"

	"github.com/vientrlenh/vox-streaming/internal/healthcheck"
	healthv1 "github.com/vientrlenh/vox-streaming/pkg/pb/health/v1"
	"go.uber.org/zap"
)

type HealthServer struct {
	healthv1.UnimplementedHealthServiceServer
	checker *healthcheck.HealthChecker
	logger *zap.Logger
}


func NewHealthServer(logger *zap.Logger, checker *healthcheck.HealthChecker) *HealthServer{
	return &HealthServer{
		logger: logger, 
		checker: checker,
	}
}

func (s *HealthServer) Ping(ctx context.Context, _ *healthv1.HealthRequest) (*healthv1.HealthResponse, error) {
	ok, _ := s.checker.CheckAll(ctx)
	if !ok {
		return &healthv1.HealthResponse{
			Alive: false, 
			Message: "streaming service is not ready",
		}, nil
	}
	return &healthv1.HealthResponse{
		Alive: true, 
		Message: "ok",
	}, nil
}

