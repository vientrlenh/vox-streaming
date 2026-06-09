package storage

import "time"

type Config struct {
	Endpoint        string
	AccessKey       string
	SecretKey       string
	UseSSL          bool
	Region          string
	FrameBucket     string
	RecordingBucket string
	PresignExpiry   time.Duration
}

func DefaultConfig(endpoint, accessKey, secretKey string) Config {
	return Config{
		Endpoint: endpoint, 
		AccessKey: accessKey, 
		SecretKey: secretKey, 
		UseSSL: false, 
		Region: "ap-southeast-2", 
		FrameBucket: "vox-frames", 
		RecordingBucket: "vox-recordings",
		PresignExpiry: 15 * time.Minute,
	}
}