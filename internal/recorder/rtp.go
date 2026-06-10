package recorder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"go.uber.org/zap"
)

// record H.264 video through ffmpeg and Opus audio (OGG) from WebRTC tracks
// Tier 2: server-side recording to reflect Tier 1 (client segments)
type RTPRecorder struct {
	roomID 	string
	streamID string
	storage *storage.Client
	logger *zap.Logger

	// video: long-running ffmpeg process reads H.264 Annex-B from stdin
	ffmpegCmd 	*exec.Cmd 
	videoIn io.WriteCloser
	videoTmpPath string
	videoMu sync.Mutex
	hasVideo bool 

	// audio: OGG Opus writer
	audioFile *os.File
	audioTmpPath string
	oggWriter *OGGWriter
	audioMu 	sync.Mutex
	hasAudio bool
}

func NewRTPRecorder(roomID, streamID string, storage *storage.Client, logger *zap.Logger) (*RTPRecorder, error) {
	r := &RTPRecorder{
		roomID: roomID, 
		streamID: streamID, 
		storage: storage, 
		logger: logger,
	}
	if err := r.initVideo(); err != nil {
		return nil, fmt.Errorf("init video recorder: %w", err)
	}
	return r, nil
}

func (r *RTPRecorder) initVideo() error {
	f, err := os.CreateTemp("", "vox-video-*.webm")
	if err != nil {
		return err
	}
	f.Close()
	r.videoTmpPath = f.Name()

	// long-running ffmpeg: reads H.264 Annex-B from stdin, write WebM
	// -y overwrites the temp file created above
	r.ffmpegCmd = exec.Command("ffmpeg", 
		"-hide_banner", "-loglevel", "error", 
		"-f", "h264", "-i", "pipe:0",
		"-c:v", "copy", 
		"-y", r.videoTmpPath,
	)
	pipe, err := r.ffmpegCmd.StdinPipe()
	if err != nil {
		os.Remove(r.videoTmpPath)
		return err
	}
	r.videoIn = pipe
	if err := r.ffmpegCmd.Start(); err != nil {
		os.Remove(r.videoTmpPath)
		return fmt.Errorf("ffmpeg start: %w", err)
	}
	return nil
}


// initialize the OGG audio writer, must be called before WriteAudioPacket
// channels: 1 = mono (typical for speech), 2 = stereo
func (r *RTPRecorder) StartAudio(channels uint8) error {
	r.audioMu.Lock()
	defer r.audioMu.Unlock()

	if r.hasAudio {
		return nil
	}
	f, err := os.CreateTemp("", "vox-audio-*.ogg")
	if err != nil {
		return err
	}
	r.audioFile = f
	r.audioTmpPath = f.Name()
	ow, err := NewOGGWriter(f, channels)
	if err != nil {
		f.Close()
		os.Remove(r.audioTmpPath)
		return err
	}
	r.oggWriter = ow
	r.hasAudio = true
	return nil
}


// write a complete picture's NAL units to the video stream
// called from FrameExtractor NALSink for every picture (IDR and non-IDR)
func (r *RTPRecorder) WriteVideoNALs(nals [][]byte) {
	r.videoMu.Lock()
	defer r.videoMu.Unlock()

	if r.videoIn == nil {
		return
	}

	r.hasVideo = true
	for _, nal := range nals {
		r.videoIn.Write(annexBStartCode)
		r.videoIn.Write(nal)
	}
}


// write a single Opus RTP payload
// sampleCount: number of samples in this packet (typically 960 for 20ms at 48kHz)
func (r *RTPRecorder) WriteAudioPacket(payload []byte, sampleCount uint32) {
	r.audioMu.Lock()
	defer r.audioMu.Unlock()
	if !r.hasAudio || r.oggWriter == nil {
		return
	}
	if err := r.oggWriter.WritePacket(payload, sampleCount); err != nil {
		r.logger.Warn("ogg write failed", zap.Error(err))
	}
}

func (r *RTPRecorder) Finalize(ctx context.Context) (string, error) {
	r.closeVideo()
	r.closeAudio()

	// nothing written, no upload
	if !r.hasVideo {
		r.cleanup()
		return "", nil
	}

	stat, err := os.Stat(r.videoTmpPath)
	if err != nil || stat.Size() < 100 {
		r.cleanup()
		return "", nil
	}

	finalPath, err := r.mux(ctx)
	if err != nil {
		r.cleanup()
		return "", fmt.Errorf("mux: %w", err)
	}
	defer os.Remove(finalPath)
	defer r.cleanup()

	f, err := os.Open(finalPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	key, err := r.storage.UploadRecordingStream(ctx, r.roomID, r.streamID, f)
	if err != nil {
		return "", fmt.Errorf("upload recording: %w", err)
	}
	url, err := r.storage.PresignRecording(ctx, key, r.storage.PresignExpiry())
	if err != nil {
		r.logger.Warn("presign recording failed, returning key", zap.Error(err))
		return key, nil
	}
	return url, nil
}

// discard the recording without uploading, use when stream ends in failure
func (r *RTPRecorder) Close() {
	r.closeVideo()
	r.closeAudio()
	r.cleanup()
}

func (r *RTPRecorder) closeVideo() {
	r.videoMu.Lock()
	defer r.videoMu.Unlock()

	if r.videoIn != nil {
		r.videoIn.Close()
		r.videoIn = nil
	}
	if r.ffmpegCmd != nil && r.ffmpegCmd.Process != nil {
		if err := r.ffmpegCmd.Wait(); err != nil {
			r.logger.Warn("ffmpeg video exited with error", zap.Error(err))
		}
	}
}

func (r *RTPRecorder) closeAudio() {
	r.audioMu.Lock()
	defer r.audioMu.Unlock()

	if r.oggWriter != nil {
		r.oggWriter.Close()
		r.oggWriter = nil
	}
	if r.audioFile != nil {
		r.audioFile.Close()
		r.audioFile = nil
	}
}

func (r *RTPRecorder) cleanup() {
	if r.videoTmpPath != "" {
		os.Remove(r.videoTmpPath)
	}
	if r.audioTmpPath != "" {
		os.Remove(r.audioTmpPath)
	}
}

func (r *RTPRecorder) mux(ctx context.Context) (string, error) {
	out, err := os.CreateTemp("", "vox-recording-*.webm")
	if err != nil {
		return "", err
	}
	outPath := out.Name()
	out.Close()

	args := []string{
		"-hide_banner", "-loglevel", "error", 
		"-i", r.videoTmpPath,
	}
	if r.hasAudio && r.audioTmpPath != "" {
		args = append(args, "-i", r.audioTmpPath)
	}
	args = append(args, "-c:v", "copy")
	if r.hasAudio {
		args = append(args, "-c:a", "copy")
	}
	args = append(args, "-y", outPath)

	var errBuf bytes.Buffer
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		os.Remove(outPath)
		return "", fmt.Errorf("ffmpeg mux: %w: %s", err, errBuf.String())
	}
	return outPath, nil
}
 