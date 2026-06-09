package client

import (
	"context"
	"fmt"
	"time"

	examv1 "github.com/vientrlenh/vox-streaming/pkg/pb/exam/v1"
	"github.com/vientrlenh/vox-streaming/pkg/auth"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type ExamClientConfig struct {
	Addr 	string 
	CAFile  string
	Token   string
}

type ExamClient struct {
	conn *grpc.ClientConn
	client examv1.ExamServiceClient
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
		conn: conn, 
		client: examv1.NewExamServiceClient(conn),
		logger: logger,
	}, nil
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