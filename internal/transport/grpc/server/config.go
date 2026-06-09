package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strings"

	alertv1 "github.com/vientrlenh/vox-streaming/pkg/pb/alert/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ServerConfig struct {
	Addr string
	CertFile string
	KeyFile string
	CAFile string
	APIKey string
}

type Server struct {
	grpcServer *grpc.Server
	listener net.Listener
	logger *zap.Logger
}

func NewServer(cfg ServerConfig, alertServer *AlertServer, logger *zap.Logger) (*Server, error) {
	lis , err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	var serverOpts []grpc.ServerOption

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		creds, err := loadServerTLS(cfg)
		if err != nil {
			return nil, fmt.Errorf("grpc tls: %w", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
		logger.Info("grpc tls enabled")
	} else {
		logger.Warn("grpc running without TLS - insecure mode")
	}

	if cfg.APIKey != "" {
		serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(apiKeyInterceptor(cfg.APIKey)))
	}
	
	s := grpc.NewServer(serverOpts...)
	alertv1.RegisterAlertServiceServer(s, alertServer)
	return &Server{
		grpcServer: s, 
		listener: lis, 
		logger: logger,
	}, nil
}

func loadServerTLS(cfg ServerConfig) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load keypair: %w", err)
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	// yêu cầu client cũng phải có cert (khi AI service nằm trong cùng mạng nội bộ)
	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca: %w", err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caPEM)
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsCfg), nil
}

func (s *Server) Serve() error {
	return s.grpcServer.Serve(s.listener)
}

func (s *Server) Shutdown() {
	s.logger.Info("grpc server shutting down")
	s.grpcServer.GracefulStop()
	s.logger.Info("grpc server stopped")
}


func apiKeyInterceptor(expectedKey string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		values := md.Get("authorization")
		if len(values) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing authorization header")
		}

		token := strings.TrimPrefix(values[0], "Bearer ")
		if token != expectedKey {
			return nil, status.Error(codes.PermissionDenied, "invalid token")
		}

		return handler(ctx, req)
	}
}
