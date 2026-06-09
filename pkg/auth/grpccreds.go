package auth

import (
	"context"

	"google.golang.org/grpc/credentials"
)

type serviceToken struct {
	token      string
	requireTLS bool
}

func NewServiceToken(token string, requireTLS bool) credentials.PerRPCCredentials {
	return &serviceToken{
		token: token, 
		requireTLS: requireTLS,
	}
}

func (t *serviceToken) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + t.token,
	}, nil
}

func (t *serviceToken) RequireTransportSecurity() bool {
	return t.requireTLS
}