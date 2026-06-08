package queue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)

func LogOnlyAlert(logger *zap.Logger) AlertFunc {
	return func(alert Alert) {
		logger.Error("KAFKA ALERT - action required",
			zap.String("level", string(alert.Level)),
			zap.String("topic", alert.Topic),
			zap.String("group_id", alert.GroupID),
			zap.String("message", alert.Message),
			zap.Int64("value", alert.Value),
			zap.Int64("threshold", alert.Threshold),
			zap.Time("at", alert.At),
		)
	}
}

func SlackAlert(webhookURL string, logger *zap.Logger) AlertFunc {
	return func(alert Alert) {
		emoji := ":warning:"
		color := "#FFC107"
		if alert.Level == AlertCritical {
			emoji = ":red_circle:"
			color = "#DC3545"
		}

		payload := map[string]any{
			"attachments": []map[string]any{
				{
					"color": color,
					"blocks": []map[string]any{
						{
							"type": "section",
							"text": map[string]string{
								"type": "mrkdwn",
								"text": fmt.Sprintf("%s *[%s] Kafka Alert - %s*\n%s", emoji, alert.Level, alert.Topic, alert.Message),
							},
						},
						{
							"type": "section",
							"fields": []map[string]string{
								{
									"type": "mrkdwn",
									"text": fmt.Sprintf("*Topic:*\n`%s`", alert.Topic),
								},
								{
									"type": "mrkdwn",
									"text": fmt.Sprintf("*Group:*\n`%s`", alert.GroupID),
								},
								{
									"type": "mrkdwn",
									"text": fmt.Sprintf("*Value:*\n%d", alert.Value),
								},
								{
									"type": "mrkdwn",
									"text": fmt.Sprintf("*Threshold:*\n%d", alert.Threshold),
								},
								{
									"type": "mrkdwn",
									"text": fmt.Sprintf("*Time:*\n%s", alert.At.Format(time.RFC3339)),
								},
							},
						},
					},
				},
			},
		}

		body, _ := json.Marshal(payload)
		res, err := http.Post(webhookURL, "application/json", bytes.NewReader(body))
		if err != nil {
			logger.Error("slack alert failed", zap.Error(err))
			return
		}
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			logger.Error("slack alert non-200", zap.Int("status", res.StatusCode))
		}
	}
}

func ChainAlert(fns ...AlertFunc) AlertFunc {
	return func(alert Alert) {
		for _, fn := range fns {
			fn(alert)
		}
	}
}
