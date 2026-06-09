package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
	"go.uber.org/zap"
)

type Client struct {
	s3 *s3.Client
	presign *s3.PresignClient
	cfg Config
	logger *zap.Logger
}

func NewClient(cfg Config, logger *zap.Logger) (*Client, error) {
	resolver := credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")

	awsCfg, err := awscfg.LoadDefaultConfig(
		context.Background(), 
		awscfg.WithRegion(cfg.Region), 
		awscfg.WithCredentialsProvider(resolver),
	)
	if err != nil {
		return nil, fmt.Errorf("storage config: %w", err)
	}

	scheme := "http"
	if cfg.UseSSL {
		scheme = "https"
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(scheme + "://" + cfg.Endpoint)
			o.UsePathStyle = true
		}
	})

	return &Client{
		s3: s3Client, 
		presign: s3.NewPresignClient(s3Client),
		cfg: cfg, 
		logger: logger,
	}, nil
}

func (c *Client) EnsureBuckets(ctx context.Context) error {
	specs := []struct {
		bucket string
		retentionDays int32
	}{
		{c.cfg.FrameBucket, 7},
		{c.cfg.RecordingBucket, 365},
	}

	for _, spec := range specs {
		if err := c.ensureBucket(ctx, spec.bucket, spec.retentionDays); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) ensureBucket(ctx context.Context, bucket string, retentionDays int32) error {
	_, err := c.s3.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		// 403 -> bucket tồn tại nhưng không có quyền truy cập
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "403" {
			return fmt.Errorf("bucket %s exists but access denied", bucket)
		}

		// lỗi do không tìm thấy bucket
		var notFound *types.NotFound
		var noSuchBucket *types.NoSuchBucket
		if !errors.As(err, &notFound) && !errors.As(err, &noSuchBucket) {
			return fmt.Errorf("check bucket %s: %w", bucket, err)
		}

		// không có bucket - tạo mới
		if _, err := c.s3.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucket),
			
		}); err != nil {
			return fmt.Errorf("create bucket %s: %w", bucket, err)
		}
		c.logger.Info("bucket created", zap.String("bucket", bucket))
	}

	_, err = c.s3.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{{
				ID: aws.String("auto-expire"),
				Status: types.ExpirationStatusEnabled, 
				Filter: &types.LifecycleRuleFilter{
					Prefix: aws.String(""),
				},
				Expiration: &types.LifecycleExpiration{
					Days: aws.Int32(retentionDays),
				},
			}},
		},
	})
	if err != nil {
		c.logger.Warn("set lifecycle failed", 
			zap.String("bucket", bucket),
			zap.Error(err),
		)
	}
	return nil
}

func (c *Client) UploadFrame(ctx context.Context, roomID, streamID string, seq int64, jpegData []byte) (string, error) {
	key := frameKey(roomID, streamID, seq)
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.cfg.FrameBucket),
		Key: aws.String(key), 
		Body: bytes.NewReader(jpegData),
		ContentType: aws.String("image/jpeg"),
	})
	if err != nil {
		return "", fmt.Errorf("upload frame: %w", err)
	}
	return key, nil
}

func (c *Client) PresignFrame(ctx context.Context, key string, expiry time.Duration) (string, error) {
	req, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.FrameBucket), 
		Key: aws.String(key),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("presign recording: %w", err)
	}
	return req.URL, nil
}

func (c *Client) UploadRecording(ctx context.Context, roomID, streamID string, data []byte, contentType string) (string, error) {
	key := recordingKey(roomID, streamID, contentType)
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.cfg.RecordingBucket),
		Key: aws.String(key),
		Body: bytes.NewReader(data), 
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return "", fmt.Errorf("upload recording: %w", err)
	}
	return key, nil
}

func (c *Client) PresignRecording(ctx context.Context, key string, expiry time.Duration) (string, error) {
	req, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.RecordingBucket), 
		Key: aws.String(key),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("presign recording: %w", err)
	}
	return req.URL, nil
}

// padding 10 chữ số -> sort lexicographic đúng thứ tự cho playback review
// rooms/{roomID}/streams/{streamID}/{seq:010d}.jpg
func frameKey(roomID, streamID string, seq int64) string {
	return fmt.Sprintf("room/%s/streams/%s/%010d.jpg", roomID, streamID, seq)
}

// rooms/{roomID}/streams/{streamID}.webm (hoặc .mp4)
func recordingKey(roomID, streamID, contentType string) string {
	ext := "webm"
	if contentType == "video/mp4" {
		ext = "mp4"
	}
	return fmt.Sprintf("rooms/%s/streams/%s.%s", roomID, streamID, ext)
}