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

	pwebrtc "github.com/pion/webrtc/v4"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/queue"
	webrtctransport "github.com/vientrlenh/vox-streaming/internal/transport/webrtc"
	"github.com/vientrlenh/vox-streaming/internal/usecase"
	"github.com/vientrlenh/vox-streaming/pkg/auth"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:9092"
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

	if err := queue.WaitForKafka(startupCtx, kafkaCfg.Brokers, logger); err != nil {
		logger.Fatal("kafka unavailable", zap.Error(err))
	}

	if err := queue.EnsureTopics(startupCtx, kafkaCfg.Brokers, logger); err != nil {
		logger.Fatal("ensure topics failed", zap.Error(err))
	}

	publisher, err := queue.NewPublisher(kafkaCfg, logger)
	if err != nil {
		logger.Fatal("publisher init failed", zap.Error(err))
	}


	streamUseCase := usecase.NewStreamUseCase(publisher, logger)
	
	iceServers := buildICEServers()
	frameIntervalSecs, _ := strconv.Atoi(os.Getenv("FRAME_INTERVAL_SECS"))
	if frameIntervalSecs == 0 {
		frameIntervalSecs = 5
	}

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
	monitorUseCase := usecase.NewMonitorUseCase(sessionRegistry, broadCaster, logger)



	webrtcHandler := webrtctransport.NewHandler(
		webrtctransport.PeerConfig{
			ICEServers: iceServers,
			FrameInterval: time.Duration(frameIntervalSecs) * time.Second, 
		},
		streamUseCase, 
		allowedOrigins,
		logger,
		jwtValidator,
		broadCaster,
	)

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/ws/stream", webrtcHandler.ServeStream)
		mux.HandleFunc("/ws/monitor", webrtcHandler.ServeMonitor)

		addr := os.Getenv("WEBRTC_ADDR")
		if addr == "" {
			addr = ":8080"
		}
		logger.Info("webrtc signaling server started", zap.String("addr", addr))
		if err := http.ListenAndServe(addr, mux); err != nil {
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

	frameConsumer := queue.NewConsumer(
		kafkaCfg, 
		domain.TopicFrameReady, 
		handleFrameReady(logger, broadCaster), 
		logger, 
		&queue.ConsumerOptions{
			MaxRetries: 3, 
			RetryDelay: 500 * time.Millisecond, 
			DLQ: dlqWriter,
		},
	)

	streamEndedConsumer := queue.NewConsumer(
		kafkaCfg, 
		domain.TopicStreamEnded, 
		handleStreamEnded(logger),
		logger, 
		&queue.ConsumerOptions{
			MaxRetries: 5, 
			RetryDelay: time.Second, 
			DLQ: dlqWriter,
		},
	)

	monitor := queue.NewMonitor(
		[]*queue.Consumer{ frameConsumer, streamEndedConsumer },
		dlqWriter, 
		queue.DefaultAlertConfigs, 
		alertFn, 
		kafkaCfg.GroupID, 
		logger,
	)

	frameConsumer.SetMonitor(monitor)
	streamEndedConsumer.SetMonitor(monitor)

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	go func() {
		if err := frameConsumer.Run(runCtx); err != nil {
			logger.Error("frame consumer error", zap.Error(err))
		}
	}()

	go func() {
		if err := streamEndedConsumer.Run(runCtx); err != nil {
			logger.Error("stream ended consumer error", zap.Error(err))
		}
	}()

	go monitor.Run(runCtx)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		logger.Info("metric server started", zap.String("addr", ":9090"))
		if err := http.ListenAndServe(":9090", mux); err != nil {
			logger.Error("metrics server error", zap.Error(err))
		}
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
		frameConsumer.Close()
		streamEndedConsumer.Close()
		publisher.Close()
		dlqWriter.Close()
		close(done)
	}()

	select {
	case <- done:
		logger.Info("grateful shutdown completed")
	case <-shutdownCtx.Done():
		logger.Warn("shutdown timeout - force close")
	}
}


func handleFrameReady(logger *zap.Logger, bc *webrtctransport.RedisBroadcaster) queue.HandlerFunc {
	return func(ctx context.Context, msg kafka.Message) error {
		var event domain.FrameReadyEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal frame event: %w", err)
		}

		// lưu kết quả vào db và notify cho spring boot service
		// ...
		bc.Publish(ctx, event.RoomID, webrtctransport.FrameNotification{
			StreamID: event.StreamID, 
			StreamType: event.StreamType, 
			FrameURL: event.FrameURL,
			SequenceNo: event.SequenceNo,
		})
		logger.Info("processing frame event", 
			zap.String("stream_id", event.StreamID), 
			zap.String("room_Id", event.RoomID), 
			zap.Int64("seq", event.SequenceNo),
		)

		// xử lý nghiệp vụ (thêm sau)
		return nil
	}
}

func handleStreamEnded(logger *zap.Logger) queue.HandlerFunc {
	return func(ctx context.Context, msg kafka.Message) error {
		var event domain.StreamEndedEvent

		if err := json.Unmarshal(msg.Value, &event); err != nil {
			return fmt.Errorf("unmarshal stream ended event: %w", err)
		}

		logger.Info("stream ended", 
			zap.String("stream_id", event.StreamID), 
			zap.String("recording_url", event.RecordingURL), 
			zap.Int64("duration_secs", event.Duration),
		)

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
			Username: os.Getenv("TURN_USERNAME"),
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
	for _, o := range strings.Split(raw, ",") {
		if o = strings.TrimSpace(o); o != "" {
			origins = append(origins, o)
		}
	}
	return origins
}