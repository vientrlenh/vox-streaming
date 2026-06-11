package client

import (
	"context"
	"fmt"
	"time"

	examv1 "github.com/vientrlenh/vox-streaming/pkg/pb/exam/v1"
	"github.com/vientrlenh/vox-streaming/pkg/auth"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type ExamClientConfig struct {
	Addr 	string 
	CAFile  string
	Token   string
}

type ExamClient struct {
	conn   *grpc.ClientConn
	client examv1.ExamServiceClient
	addr   string
	logger *zap.Logger
}

func NewExamClient(cfg ExamClientConfig, logger *zap.Logger) (*ExamClient, error) {
	var opts []grpc.DialOption

	if cfg.CAFile != "" {
		creds, err := credentials.NewClientTLSFromFile(cfg.CAFile, "")
		if err != nil {
			return nil, fmt.Errorf("load ca cert: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		logger.Warn("exam grpc client running without TLS")
	}

	if cfg.Token != "" {
		serviceToken := auth.NewServiceToken(
			cfg.Token, 
			cfg.CAFile != "",
		)
		opts = append(opts, grpc.WithPerRPCCredentials(serviceToken))
	}

	conn, err := grpc.NewClient(cfg.Addr, opts...)
	if err != nil {
		return nil, err
	}
	return &ExamClient{
		conn:   conn,
		client: examv1.NewExamServiceClient(conn),
		addr:   cfg.Addr,
		logger: logger,
	}, nil
}

func (c *ExamClient) Addr() string { return c.addr }

// Ping reports an error if the underlying gRPC connection has entered a failed state.
// gRPC connections are lazy, so Idle/Connecting/Ready all indicate healthy.
func (c *ExamClient) Ping(_ context.Context) error {
	switch state := c.conn.GetState(); state {
	case connectivity.TransientFailure, connectivity.Shutdown:
		return fmt.Errorf("connection state: %s", state)
	default:
		return nil
	}
}

func (c *ExamClient) ValidateAccess(ctx context.Context, roomID, participantID, streamType string) (allowed bool, reason string, err error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := c.client.ValidateAccess(ctx, &examv1.ValidateAccessRequest{
		RoomId: roomID, 
		ParticipantId: participantID, 
		StreamType: streamType,
	})
	if err != nil {
		return false, "", err
	}
	return resp.Allowed, resp.Reason, nil
}

func (c *ExamClient) Close() {
	if err := c.conn.Close(); err != nil {
		c.logger.Warn("exam grpc client close failed", zap.Error(err))
	}
}

func (c *ExamClient) UpdateRecording(ctx context.Context, streamID, roomID, recordingURL string, durationSecs int64) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := c.client.UpdateRecording(ctx, &examv1.UpdateRecordingRequest{
		StreamId: streamID, 
		RoomId: roomID, 
		RecordingUrl: recordingURL, 
		DurationSecs: durationSecs,
	})
	if err != nil {
		return fmt.Errorf("update recording: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("update recording rejected by exam service (stream_id=%s)", streamID)
	}
	return nil
}