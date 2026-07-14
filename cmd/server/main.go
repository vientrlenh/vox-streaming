package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	pwebrtc "github.com/pion/webrtc/v4"
	"github.com/segmentio/kafka-go"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/queue"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"github.com/vientrlenh/vox-streaming/internal/recorder/ffmpegingest"
	grpcclient "github.com/vientrlenh/vox-streaming/internal/transport/grpc/client"
	grpctransport "github.com/vientrlenh/vox-streaming/internal/transport/grpc/server"
	httpRoute "github.com/vientrlenh/vox-streaming/internal/transport/http"
	segmenttransport "github.com/vientrlenh/vox-streaming/internal/transport/segment"
	webrtctransport "github.com/vientrlenh/vox-streaming/internal/transport/webrtc"
	"github.com/vientrlenh/vox-streaming/internal/usecase"
	"github.com/vientrlenh/vox-streaming/pkg/auth"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	err := godotenv.Load()
	if err != nil {
		logger.Fatal("error loading env file")
	}

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	groupID := os.Getenv("KAFKA_CONSUMER_GROUP")
	if groupID == "" {
		groupID = "vox-streaming"
	}

	kafkaCfg := queue.NewConfig(
		queue.DefaultConfig(
			strings.Split(brokers, ","),
			groupID,
		),
		os.Getenv("KAFKA_TLS_ENABLED") == "true",
		os.Getenv("KAFKA_USERNAME"),
		os.Getenv("KAFKA_PASSWORD"),
	)

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startupCancel()

	if err := queue.WaitForKafka(startupCtx, kafkaCfg, kafkaCfg.Brokers, logger); err != nil {
		logger.Fatal("kafka unavailable", zap.Error(err))
	}

	if err := queue.EnsureTopics(startupCtx, kafkaCfg, kafkaCfg.Brokers, logger); err != nil {
		logger.Fatal("ensure topics failed", zap.Error(err))
	}

	publisher, err := queue.NewPublisher(kafkaCfg, logger)
	if err != nil {
		logger.Fatal("publisher init failed", zap.Error(err))
	}

	iceServers := buildICEServers()
	frameIntervalSecs, _ := strconv.Atoi(os.Getenv("FRAME_INTERVAL_SECS"))
	if frameIntervalSecs == 0 {
		frameIntervalSecs = 5
	}

	// Build the shared WebRTC API once. WEBRTC_UDP_PORT muxes all peer media onto
	// a single UDP port (expose just that port in containers); WEBRTC_NAT_1TO1_IP
	// advertises the server's public IP for host candidates behind a 1:1 NAT.
	webrtcUDPPort, _ := strconv.Atoi(os.Getenv("WEBRTC_UDP_PORT"))
	var natIPs []string
	if raw := os.Getenv("WEBRTC_NAT_1TO1_IP"); raw != "" {
		natIPs = strings.Split(raw, ",")
	}
	webrtcAPI, webrtcAPIClose, err := webrtctransport.NewWebRTCAPI(webrtctransport.ICEConfig{
		UDPPort:    webrtcUDPPort,
		NAT1To1IPs: natIPs,
	}, logger)
	if err != nil {
		logger.Fatal("webrtc api init failed", zap.Error(err))
	}
	defer webrtcAPIClose()

	aiRelayQueueSize, _ := strconv.Atoi(os.Getenv("AI_RELAY_QUEUE_SIZE"))

	allowedOrigins := parseAllowedOrigins()

	jwtValidator, err := auth.NewValidator()
	if err != nil {
		logger.Fatal("jwt validator failed", zap.Error(err))
	}

	redisCfg := cache.DefaultConfig(os.Getenv("REDIS_ADDR"))
	redisCfg.Password = os.Getenv("REDIS_PASSWORD")
	redisClient, err := cache.NewClient(redisCfg)
	if err != nil {
		logger.Fatal("redis connect failed", zap.Error(err))
	}
	defer redisClient.Close()

	sessionRegistry := cache.NewSessionRegistry(redisClient)
	broadCaster := webrtctransport.NewRedisBroadcaster(redisClient, logger)

	streamUseCase := usecase.NewStreamUseCase(publisher, sessionRegistry, logger)
	monitorUseCase := usecase.NewMonitorUseCase(sessionRegistry, broadCaster, broadCaster, publisher, logger)

	alertServer := grpctransport.NewAlertServer(monitorUseCase, logger)

	grpcAddr := os.Getenv("GRPC_ADDR")
	if grpcAddr == "" {
		grpcAddr = ":9091"
	}

	grpcServer, err := grpctransport.NewServer(grpctransport.ServerConfig{
		Addr:     grpcAddr,
		CertFile: os.Getenv("GRPC_CERT_FILE"),
		KeyFile:  os.Getenv("GRPC_KEY_FILE"),
		CAFile:   os.Getenv("GRPC_CA_FILE"),
		APIKey:   os.Getenv("GRPC_SERVICE_TOKEN"),
	}, alertServer, logger)
	if err != nil {
		logger.Fatal("grpc server create failed", zap.Error(err))
	}

	examClient, err := grpcclient.NewExamClient(grpcclient.ExamClientConfig{
		Addr:   os.Getenv("EXAM_SERVICE_GRPC_ADDR"),
		CAFile: os.Getenv("EXAM_SERVICE_CA_FILE"),
		Token:  os.Getenv("GRPC_SERVICE_TOKEN"),
	}, logger)
	if err != nil {
		logger.Fatal("grpc exam client create failed", zap.Error(err))
	}

	go func() {
		logger.Info("grpc server started", zap.String("addr", grpcAddr))
		if err := grpcServer.Serve(); err != nil {
			logger.Error("grpc server error", zap.Error(err))
		}
	}()

	storageClient := ensureStorage(startupCtx, logger)
	segmentRegistry := cache.NewSegmentRegistry(redisClient)
	segmentUseCase := usecase.NewSegmentUseCase(storageClient, segmentRegistry, sessionRegistry, logger)

	// grace period the assembler waits after stream.ended for a client
	// completion signal before assembling with whatever segments have arrived
	assemblyGraceSecs, _ := strconv.Atoi(os.Getenv("ASSEMBLY_GRACE_PERIOD_SECS"))
	if assemblyGraceSecs == 0 {
		assemblyGraceSecs = 90
	}
	assemblerUseCase := usecase.NewAssemblerUseCase(
		storageClient, examClient, segmentRegistry,
		time.Duration(assemblyGraceSecs)*time.Second,
		logger,
	)
	segmentHandler := segmenttransport.NewSegmentHandler(segmentUseCase, assemblerUseCase, jwtValidator, logger)

	ffmpegIngestOpts := buildFFmpegIngestOptions(logger)

	webrtcHandler := webrtctransport.NewHandler(
		webrtctransport.PeerConfig{
			API:           webrtcAPI,
			ICEServers:    iceServers,
			FrameInterval: time.Duration(frameIntervalSecs) * time.Second,
			TempDir:       os.Getenv("SEGMENT_TEMP_DIR"),
			AIRelay: webrtctransport.AIRelayOptions{
				Enabled:   os.Getenv("AI_RELAY_ENABLED") == "true",
				URL:       os.Getenv("AI_WEBRTC_URL"),
				QueueSize: aiRelayQueueSize,
			},
			FFmpegIngest: ffmpegIngestOpts,
		},
		streamUseCase,
		monitorUseCase,
		allowedOrigins,
		logger,
		jwtValidator,
		broadCaster,
		examClient,
		storageClient, 
		segmentRegistry, 
	)

	addr := os.Getenv("WEBRTC_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	mux := http.NewServeMux()
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		httpRoute.Register(mux, webrtcHandler, segmentHandler)
		logger.Info("webrtc signaling server started", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("webrtc server error", zap.Error(err))
		}
	}()

	mainTopics := []string{
		domain.TopicFrameReady,
		domain.TopicStreamStarted,
		domain.TopicStreamEnded,
		domain.TopicRoomClosed,
	}

	dlqWriter, err := queue.NewDLQWriter(kafkaCfg, mainTopics, logger)
	if err != nil {
		logger.Fatal("dlq writer init failed", zap.Error(err))
	}

	slackWebhook := os.Getenv("SLACK_WEBHOOK_URL")
	var alertFn queue.AlertFunc
	if slackWebhook != "" {
		alertFn = queue.ChainAlert(
			queue.LogOnlyAlert(logger),
			queue.SlackAlert(slackWebhook, logger),
		)
	} else {
		alertFn = queue.LogOnlyAlert(logger)
	}

	frameConverterCfg := kafkaCfg
	frameConverterCfg.GroupID = "vox-frame-converter"
	maxConv, _ := strconv.Atoi(os.Getenv("FRAME_CONVERT_CONCURRENCY"))
	frameConvertUseCase := usecase.NewFrameConvertUseCase(storageClient, broadCaster, maxConv, logger)

	frameConverterConsumer := queue.NewConsumer(
		frameConverterCfg,
		domain.TopicFrameReady,
		handleFrameConvert(frameConvertUseCase),
		logger,
		&queue.ConsumerOptions{
			MaxRetries:         2,
			RetryDelay:         300 * time.Millisecond,
			DLQ:                dlqWriter,
			CommitOnDLQFailure: true,
			MaxDLQFails:        0,
		},
	)

	streamStartedConsumer := queue.NewConsumer(
		kafkaCfg,
		domain.TopicStreamStarted,
		handleStreamStarted(logger, monitorUseCase),
		logger,
		&queue.ConsumerOptions{
			MaxRetries:         5,
			RetryDelay:         time.Second,
			DLQ:                dlqWriter,
			CommitOnDLQFailure: false,
			MaxDLQFails:        10,
		},
	)
	streamEndedConsumer := queue.NewConsumer(
		kafkaCfg,
		domain.TopicStreamEnded,
		handleStreamEnded(logger, monitorUseCase),
		logger,
		&queue.ConsumerOptions{
			MaxRetries:         5,
			RetryDelay:         time.Second,
			DLQ:                dlqWriter,
			CommitOnDLQFailure: false,
			MaxDLQFails:        10,
		},
	)

	assemblerKafkaCfg := kafkaCfg
	assemblerKafkaCfg.GroupID = "vox-assembler"

	assemblerConsumer := queue.NewConsumer(
		assemblerKafkaCfg,
		domain.TopicStreamEnded,
		handleAssembly(assemblerUseCase),
		logger,
		&queue.ConsumerOptions{
			MaxRetries:         10,
			RetryDelay:         5 * time.Second,
			DLQ:                dlqWriter,
			CommitOnDLQFailure: false,
			MaxDLQFails:        3,
		},
	)

	allConsumers := []*queue.Consumer{
		frameConverterConsumer,
		streamStartedConsumer,
		streamEndedConsumer,
		assemblerConsumer,
	}
	alertConfigs := queue.BuildAlertConfigs(
		allConsumers,
		queue.DefaultAlertConfigs,
		map[queue.ConsumerKey]queue.AlertConfig{
			{Topic: domain.TopicStreamEnded, GroupID: "vox-assembler"}: {
				LagThreshold: 50,
				LagDuration:  5 * time.Minute,
				DLQThreshold: 3,
			},
		},
	)

	monitor := queue.NewMonitor(
		allConsumers,
		dlqWriter,
		alertConfigs,
		alertFn,
		logger,
	)

	frameConverterConsumer.SetMonitor(monitor)
	streamStartedConsumer.SetMonitor(monitor)
	streamEndedConsumer.SetMonitor(monitor)
	assemblerConsumer.SetMonitor(monitor)

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	go func() {
		if err := frameConverterConsumer.Run(runCtx); err != nil {
			logger.Error("frame consumer error", zap.Error(err))
		}
	}()

	go func() {
		if err := streamStartedConsumer.Run(runCtx); err != nil {
			logger.Error("stream started consumer error", zap.Error(err))
		}
	}()

	go func() {
		if err := streamEndedConsumer.Run(runCtx); err != nil {
			logger.Error("stream ended consumer error", zap.Error(err))
		}
	}()

	go func() {
		if err := assemblerConsumer.Run(runCtx); err != nil {
			logger.Error("assembler consumer error", zap.Error(err))
		}
	}()

	go monitor.Run(runCtx)

	hc := httpRoute.NewHealthChecker(redisClient, kafkaCfg.Brokers, kafkaCfg, storageClient, examClient)
	go func() {
		httpRoute.RunMetric(hc, logger)
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	sig := <-quit
	logger.Info("shutdown signal received", zap.String("signal", sig.String()))

	runCancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		frameConverterConsumer.Close()
		streamStartedConsumer.Close()
		streamEndedConsumer.Close()
		assemblerConsumer.Close()
		publisher.Close()
		dlqWriter.Close()
		grpcServer.Shutdown()
		srv.Shutdown(shutdownCtx)
		close(done)
	}()

	select {
	case <-done:
		logger.Info("grateful shutdown completed")
	case <-shutdownCtx.Done():
		logger.Warn("shutdown timeout - force close")
	}
}

func handleStreamStarted(logger *zap.Logger, mu *usecase.MonitorUseCase) queue.HandlerFunc {
	return func(ctx context.Context, msg kafka.Message) error {
		var event domain.StreamStartedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal stream started: %w", err)
		}
		logger.Info(
			"stream started",
			zap.String("stream_id", event.StreamID),
			zap.String("room_id", event.RoomID),
			zap.String("stream_type", event.StreamType),
		)

		mu.NotifyJoined(ctx, event.RoomID, event.ParticipantID, event.StreamID, event.StreamType)

		return nil
	}
}

func handleStreamEnded(logger *zap.Logger, mu *usecase.MonitorUseCase) queue.HandlerFunc {
	return func(ctx context.Context, msg kafka.Message) error {
		var event domain.StreamEndedEvent

		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal stream ended event: %w", err)
		}

		logger.Info("stream ended",
			zap.String("streamId", event.StreamID),
			zap.Int("segmentsCount", len(event.SegmentKeys)),
			zap.Int64("durationSecs", event.Duration),
		)

		mu.NotifyLeft(ctx, event.RoomID, event.ParticipantID, event.StreamID, event.StreamType)

		// notify qua Spring Boot bằng GRPC để cập nhật dữ liệu trạng thái vào DB
		// ...
		return nil
	}
}

func buildICEServers() []pwebrtc.ICEServer {
	var servers []pwebrtc.ICEServer

	stunURLs := os.Getenv("STUN_URLS")
	if stunURLs == "" {
		stunURLs = "stun:stun.l.google.com:19302"
	}
	servers = append(servers, pwebrtc.ICEServer{
		URLs: strings.Split(stunURLs, ","),
	})

	turnURL := os.Getenv("TURN_URL")
	if turnURL != "" {
		servers = append(servers, pwebrtc.ICEServer{
			URLs: []string{
				turnURL,
			},
			Username:   os.Getenv("TURN_USERNAME"),
			Credential: os.Getenv("TURN_CREDENTIAL"),
		})
	}
	return servers
}

func parseAllowedOrigins() []string {
	raw := os.Getenv("ALLOWED_ORIGINS")
	if raw == "" {
		raw = os.Getenv("ALLOWED_ORIGIN")
	}
	if raw == "" {
		return []string{"http://localhost:5173"}
	}
	var origins []string
	for o := range strings.SplitSeq(raw, ",") {
		if o = strings.TrimSpace(o); o != "" {
			origins = append(origins, o)
		}
	}
	return origins
}


func buildFFmpegIngestOptions(logger *zap.Logger) webrtctransport.FFmpegIngestOptions {
	if os.Getenv("FFMPEG_INGEST_ENABLED") != "true" {
		return webrtctransport.FFmpegIngestOptions{}
	}
	rangeStart, _ := strconv.Atoi(os.Getenv("FFMPEG_INGEST_PORT_RANGE_START"))
	if rangeStart == 0 {
		rangeStart = 40000
	}
	rangeEnd, _ := strconv.Atoi(os.Getenv("FFMPEG_INGEST_PORT_RANGE_END"))
	if rangeEnd == 0 {
		rangeEnd = 41999
	}
	alloc, err := ffmpegingest.NewPortAllocator(rangeStart, rangeEnd)
	if err != nil {
		logger.Warn("ffmpeg ingest bridge disabled: port allocator init failed", zap.Error(err))
		return webrtctransport.FFmpegIngestOptions{}
	}

	segmentSeconds, _ := strconv.Atoi(os.Getenv("FFMPEG_INGEST_SEGMENT_SECONDS"))
	if segmentSeconds == 0 {
		segmentSeconds = 30 // matches SegmentedRecorder's defaultSegmentDuration
	}
	maxConcurrent, _ := strconv.Atoi(os.Getenv("FFMPEG_INGEST_MAX_CONCURRENT"))
	if maxConcurrent == 0 {
		maxConcurrent = 10
	}
	maxRestartAttempts, _ := strconv.Atoi(os.Getenv("FFMPEG_INGEST_MAX_RESTART_ATTEMPTS"))
	if maxRestartAttempts == 0 {
		maxRestartAttempts = 3
	}

	logger.Info("ffmpeg ingest bridge enabled (cutover, RECORDING.md §11)",
		zap.Int("portRangeStart", rangeStart),
		zap.Int("portRangeEnd", rangeEnd),
		zap.Int("segmentSeconds", segmentSeconds),
		zap.Int("maxConcurrentRecorders", maxConcurrent),
		zap.Int("maxRestartAttempts", maxRestartAttempts),
	)
	return webrtctransport.FFmpegIngestOptions{
		Enabled:            true,
		Allocator:          alloc,
		RecordSem:          make(chan struct{}, maxConcurrent),
		SegmentSeconds:     segmentSeconds,
		MaxRestartAttempts: maxRestartAttempts,
	}
}

func ensureStorage(startupCtx context.Context, logger *zap.Logger) *storage.Client {
	storageEndpoint := os.Getenv("STORAGE_ENDPOINT")
	storageCfg := storage.DefaultConfig(
		storageEndpoint,
		os.Getenv("STORAGE_ACCESS_KEY"),
		os.Getenv("STORAGE_SECRET_KEY"),
	)
	if b := os.Getenv("STORAGE_FRAME_BUCKET"); b != "" {
		storageCfg.FrameBucket = b
	}
	if b := os.Getenv("STORAGE_RECORDING_BUCKET"); b != "" {
		storageCfg.RecordingBucket = b
	}
	storageCfg.UseSSL = os.Getenv("STORAGE_USE_SSL") == "true"
	if presignMins := os.Getenv("STORAGE_PRESIGN_MINUTES"); presignMins != "" {
		if m, err := strconv.Atoi(presignMins); err == nil {
			storageCfg.PresignExpiry = time.Duration(m) * time.Minute
		}
	}
	storageClient, err := storage.NewClient(storageCfg, logger)
	if err != nil {
		logger.Fatal("storage init failed", zap.Error(err))
	}
	if err := storageClient.EnsureBuckets(startupCtx); err != nil {
		logger.Fatal("ensure bucket failed", zap.Error(err))
	}
	return storageClient
}

func handleAssembly(uc *usecase.AssemblerUseCase) queue.HandlerFunc {
	return func(ctx context.Context, msg kafka.Message) error {
		var event domain.StreamEndedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal stream ended for assembly: %w", err)
		}
		return uc.OnStreamEnded(ctx, event.RoomID, event.StreamID)
	}
}

func handleFrameConvert(uc *usecase.FrameConvertUseCase) queue.HandlerFunc {
	return func(ctx context.Context, msg kafka.Message) error {
		var event domain.FrameReadyEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal frame for convert: %w", err)
		}
		return uc.Convert(ctx, event)
	}
}
