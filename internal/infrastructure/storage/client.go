package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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
	s3      *s3.Client
	presign *s3.PresignClient
	cfg     Config
	logger  *zap.Logger
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
		s3:      s3Client,
		presign: s3.NewPresignClient(s3Client),
		cfg:     cfg,
		logger:  logger,
	}, nil
}

// Live HLS fragments are working files for the invigilator's in-exam view, not evidence: the
// archival copy of the same stream is kept separately under ffmpeg-segments/ and recording.mp4.
// Nothing reads a fragment once the exam is over, so keeping them for the recording bucket's full
// retention stores the entire WebRTC ingest twice for a year.
//
// They cannot be targeted by prefix -- scheduleID/sessionID/streamID sit in the middle of the key
// and S3 lifecycle prefixes have no wildcard -- so they are tagged at upload instead and matched by
// tag here.
const (
	liveAssetTagKey        = "class"
	liveAssetTagValue      = "live"
	liveAssetRetentionDays = 3
	recordingRetentionDays = 365
	frameRetentionDays     = 7

	// PutObject takes the tag set URL-encoded in a single header, not as structured fields.
	liveAssetTagging = liveAssetTagKey + "=" + liveAssetTagValue
)

func (c *Client) EnsureBuckets(ctx context.Context) error {
	specs := []struct {
		bucket        string
		retentionDays int32
		// true only for buckets a browser fetches with XHR, where CORS applies: hls.js loads live
		// rewind fragments out of the recording bucket. Frames are consumed as <img> src, which is
		// not a CORS request at all.
		browserFetched bool
		// true only for the bucket that actually holds tagged live assets.
		expireLiveAssets bool
	}{
		{c.cfg.FrameBucket, frameRetentionDays, false, false},
		{c.cfg.RecordingBucket, recordingRetentionDays, true, true},
	}

	for _, spec := range specs {
		if err := c.ensureBucket(ctx, spec.bucket, spec.retentionDays, spec.browserFetched, spec.expireLiveAssets); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) ensureBucket(ctx context.Context, bucket string, retentionDays int32, browserFetched, expireLiveAssets bool) error {
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

		createInput := &s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		}
		if c.cfg.Region != "" && c.cfg.Region != "us-east-1" {
			createInput.CreateBucketConfiguration = &types.CreateBucketConfiguration{
				LocationConstraint: types.BucketLocationConstraint(c.cfg.Region),
			}
		}

		// không có bucket - tạo mới
		if _, err := c.s3.CreateBucket(ctx, createInput); err != nil {
			return fmt.Errorf("create bucket %s: %w", bucket, err)
		}
		c.logger.Info("bucket created", zap.String("bucket", bucket))
	}

	rules := []types.LifecycleRule{{
		ID:     aws.String("auto-expire"),
		Status: types.ExpirationStatusEnabled,
		Filter: &types.LifecycleRuleFilter{
			Prefix: aws.String(""),
		},
		Expiration: &types.LifecycleExpiration{
			Days: aws.Int32(retentionDays),
		},
	}}

	if expireLiveAssets {
		// This overlaps the blanket rule above, which S3 lifecycle cannot express an exception to
		// (filters have no negation). Where two expiration rules match the same object, S3 applies
		// the earlier one -- so tagged fragments expire on the short schedule.
		//
		// Worth noting for anyone porting this to another S3 implementation: if the backing store
		// resolved the overlap the other way instead, tagged fragments would simply keep the
		// bucket's full retention, which is exactly what they had before this rule existed. The
		// failure mode is "no saving", never "evidence deleted early".
		rules = append(rules, types.LifecycleRule{
			ID:     aws.String("expire-live-assets"),
			Status: types.ExpirationStatusEnabled,
			Filter: &types.LifecycleRuleFilter{
				Tag: &types.Tag{
					Key:   aws.String(liveAssetTagKey),
					Value: aws.String(liveAssetTagValue),
				},
			},
			Expiration: &types.LifecycleExpiration{
				Days: aws.Int32(liveAssetRetentionDays),
			},
		})
	}

	_, err = c.s3.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: rules,
		},
	})
	if err != nil {
		c.logger.Warn("set lifecycle failed",
			zap.String("bucket", bucket),
			zap.Error(err),
		)
	}

	if browserFetched {
		_, err = c.s3.PutBucketCors(ctx, &s3.PutBucketCorsInput{
			Bucket: aws.String(bucket),
			CORSConfiguration: &types.CORSConfiguration{
				CORSRules: []types.CORSRule{{
					AllowedMethods: []string{"GET", "HEAD"},
					// Must be "*", not the service's own ALLOWED_ORIGINS. Browsers reach these
					// objects by following the 302 that GetLiveAsset returns, and per the Fetch
					// spec a CORS request redirected to a different origin has its Origin header
					// replaced with the opaque value "null" -- so no explicit origin list can ever
					// match on the post-redirect request, and hls.js fails every fragment with
					// "TypeError: Failed to fetch". Origin is not the access control here anyway:
					// these are presigned URLs, gated by the signature and its expiry.
					AllowedOrigins: []string{"*"},
					AllowedHeaders: []string{"Range"},
					MaxAgeSeconds:  aws.Int32(3600),
				}},
			},
		})
		if err != nil {
			c.logger.Warn("set bucket cors failed",
				zap.String("bucket", bucket),
				zap.Error(err),
			)
		}
	}
	return nil
}

func (c *Client) UploadFrame(ctx context.Context, scheduleID, streamID string, seq int64, frameData []byte) (string, error) {
	key := frameKey(scheduleID, streamID, seq)
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.cfg.FrameBucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(frameData),
		ContentType: aws.String("video/h264"),
	})
	if err != nil {
		return "", fmt.Errorf("upload frame: %w", err)
	}
	return key, nil
}

func (c *Client) PresignFrame(ctx context.Context, key string, expiry time.Duration) (string, error) {
	req, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.FrameBucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("presign frame: %w", err)
	}
	return req.URL, nil
}

func (c *Client) PresignRecording(ctx context.Context, key string, expiry time.Duration) (string, error) {
	req, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.RecordingBucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", fmt.Errorf("presign recording: %w", err)
	}
	return req.URL, nil
}

// schedules/{scheduleID}/streams/{streamID}/{seq:010d}.264
func frameKey(scheduleID, streamID string, seq int64) string {
	return fmt.Sprintf("schedules/%s/streams/%s/%010d.264", scheduleID, streamID, seq)
}

func (c *Client) PresignExpiry() time.Duration {
	return c.cfg.PresignExpiry
}

func segmentKey(scheduleID, sessionID, streamID string, seq int64) string {
	return fmt.Sprintf("schedules/%s/sessions/%s/streams/%s/segments/%04d.mp4", scheduleID, sessionID, streamID, seq)
}

func (c *Client) UploadSegment(ctx context.Context, scheduleID, sessionID, streamID string, seq int64, data []byte) (string, error) {
	key := segmentKey(scheduleID, sessionID, streamID, seq)
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.cfg.RecordingBucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("video/mp4"),
	})
	if err != nil {
		return "", fmt.Errorf("upload segment: %w", err)
	}
	return key, nil
}

func (c *Client) UploadServerSegment(ctx context.Context, scheduleID, sessionID, streamID string, seq int64, r io.Reader) (string, error) {
	key := fmt.Sprintf("schedules/%s/sessions/%s/streams/%s/server-segments/%04d.mp4", scheduleID, sessionID, streamID, seq)
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.cfg.RecordingBucket),
		Key:         aws.String(key),
		Body:        r,
		ContentType: aws.String("video/mp4"),
	})
	if err != nil {
		return "", fmt.Errorf("upload server segment: %w", err)
	}
	return key, nil
}

func ffmpegSegmentKey(scheduleID, sessionID, streamID string, seq int64) string {
	return fmt.Sprintf("schedules/%s/sessions/%s/streams/%s/ffmpeg-segments/%04d.mp4", scheduleID, sessionID, streamID, seq)
}

func (c *Client) UploadFFmpegSegment(ctx context.Context, scheduleID, sessionID, streamID string, seq int64, r io.Reader) (string, error) {
	key := ffmpegSegmentKey(scheduleID, sessionID, streamID, seq)
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.cfg.RecordingBucket),
		Key:         aws.String(key),
		Body:        r,
		ContentType: aws.String("video/mp4"),
	})
	if err != nil {
		return "", fmt.Errorf("upload ffmpeg segment: %w", err)
	}
	return key, nil
}

func hlsInitKey(scheduleID, sessionID, streamID string, epoch int) string {
	return fmt.Sprintf("schedules/%s/sessions/%s/streams/%s/hls/init-%02d.mp4", scheduleID, sessionID, streamID, epoch)
}

func hlsFragmentKey(scheduleID, sessionID, streamID string, seq int64) string {
	return fmt.Sprintf("schedules/%s/sessions/%s/streams/%s/hls/%06d.m4s", scheduleID, sessionID, streamID, seq)
}

func (c *Client) UploadHLSInit(ctx context.Context, scheduleID, sessionID, streamID string, epoch int, r io.Reader) (string, error) {
	key := hlsInitKey(scheduleID, sessionID, streamID, epoch)
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.cfg.RecordingBucket),
		Key:         aws.String(key),
		Body:        r,
		ContentType: aws.String("video/mp4"),
		Tagging:     aws.String(liveAssetTagging),
	})
	if err != nil {
		return "", fmt.Errorf("upload hls init segment: %w", err)
	}
	return key, nil
}

func (c *Client) UploadHLSFragment(ctx context.Context, scheduleID, sessionID, streamID string, seq int64, r io.Reader) (string, error) {
	key := hlsFragmentKey(scheduleID, sessionID, streamID, seq)
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.cfg.RecordingBucket),
		Key:         aws.String(key),
		Body:        r,
		ContentType: aws.String("video/iso.segment"),
		Tagging:     aws.String(liveAssetTagging),
	})
	if err != nil {
		return "", fmt.Errorf("upload hls fragment: %w", err)
	}
	return key, nil
}

func FinalRecordingKey(scheduleID, sessionID, streamID string) string {
	return fmt.Sprintf("schedules/%s/sessions/%s/streams/%s/recording.mp4", scheduleID, sessionID, streamID)
}

// QualityReportKey is where a recording's measured quality signals live: beside the recording they
// describe, under the same retention, in the same bucket.
//
// Deliberately not Redis and not a log line. The signals exist to answer a question asked long
// after the exam -- why a recording is short, why it has no sound, whether the file is the one the
// client believed it captured -- and Redis keys expire while logs roll over. An object next to the
// evidence outlives both and travels with it.
func QualityReportKey(scheduleID, sessionID, streamID string) string {
	return fmt.Sprintf("schedules/%s/sessions/%s/streams/%s/quality.json", scheduleID, sessionID, streamID)
}

// UploadQualityReport stores the quality report for an already-uploaded recording.
//
// Untagged, so it keeps the recording bucket's full retention rather than the short live-asset
// window: a report that expired before the recording it describes would leave the recording
// unexplained for most of its life.
func (c *Client) UploadQualityReport(ctx context.Context, scheduleID, sessionID, streamID string, report []byte) (string, error) {
	key := QualityReportKey(scheduleID, sessionID, streamID)
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.cfg.RecordingBucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(report),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return "", fmt.Errorf("upload quality report: %w", err)
	}
	return key, nil
}

// check finalized mp4 file was assemblized and uploaded yet
// to make sure idempotency for assembler consumer
func (c *Client) RecordingExists(ctx context.Context, scheduleID, sessionID, streamID string) (bool, error) {
	key := FinalRecordingKey(scheduleID, sessionID, streamID)
	_, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.cfg.RecordingBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
			code := apiErr.ErrorCode()
			if code == "NotFound" || code == "NoSuchKey" {
				return false, nil
			}
		}
		return false, fmt.Errorf("check recording existence: %w", err)
	}
	return true, nil
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.s3.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.cfg.FrameBucket),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "403" {
			return nil // bucket exists but creds have no list perm — storage is reachable
		}
		return err
	}
	return nil
}

func (c *Client) DownloadSegmentToFile(ctx context.Context, key, dstPath string) error {
	result, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.RecordingBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("get segment %s: %w", key, err)
	}
	defer result.Body.Close()

	f, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("create dst file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, result.Body); err != nil {
		return fmt.Errorf("write segment to file: %w", err)
	}
	return nil
}

func (c *Client) UploadFinalRecording(ctx context.Context, scheduleID, sessionID, streamID string, r io.Reader) (string, error) {
	key := FinalRecordingKey(scheduleID, sessionID, streamID)
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.cfg.RecordingBucket),
		Key:         aws.String(key),
		Body:        r,
		ContentType: aws.String("video/mp4"),
	})
	if err != nil {
		return "", fmt.Errorf("upload final recording: %w", err)
	}
	return key, nil
}

func (c *Client) DownloadFrame(ctx context.Context, scheduleID, streamID string, seq int64) ([]byte, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.cfg.FrameBucket),
		Key:    aws.String(frameKey(scheduleID, streamID, seq)),
	})
	if err != nil {
		return nil, fmt.Errorf("get frame error: %w", err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (c *Client) UploadFrameJPEG(ctx context.Context, scheduleID, streamID string, seq int64, data []byte) (string, error) {
	key := fmt.Sprintf("schedules/%s/streams/%s/%010d.jpg", scheduleID, streamID, seq)
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.cfg.FrameBucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("image/jpeg"),
	})
	if err != nil {
		return "", fmt.Errorf("upload frame jpeg: %w", err)
	}
	return key, nil
}
