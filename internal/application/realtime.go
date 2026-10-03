package service

import (
	"context"
	"encoding/json"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	commonrealtime "github.com/ofm-microservices/ofm-common/pkg/realtime"
	"registration-saga-service/internal/domain"
)

// publishRegistrationNotification maps durable saga transitions to the
// client-facing realtime contract. The saga topic remains the source of truth;
// this notification is only a delivery convenience for WS clients and tests.
func (s *registrationService) publishRegistrationNotification(ctx context.Context, session domain.Session, eventType, status, errorCode string, retryable *bool, payload json.RawMessage) {
	if s.realtime == nil {
		return
	}
	notification := commonrealtime.WithOutcome(ctx, eventType, "registration", session.SessionID, session.UserID, session.SessionID, session.SessionID, status, errorCode, retryable, payload)
	notification.DeliveryScope = "registration"
	notification.UserID = ""
	body, err := json.Marshal(notification)
	if err != nil {
		s.log.Error("marshal registration realtime notification failed", logging.String("event_type", eventType), logging.String("session_id", session.SessionID), logging.Err(err))
		return
	}
	if err := s.realtime.PublishRealtime(ctx, body); err != nil {
		s.log.Error("publish registration realtime notification failed", logging.String("event_type", eventType), logging.String("session_id", session.SessionID), logging.Err(err))
		return
	}
	s.log.Info("registration realtime notification published",
		logging.String("event_type", eventType),
		logging.String("session_id", session.SessionID),
		logging.String("operation_id", notification.OperationID),
		logging.String("status", notification.Status),
	)
}

func registrationErrorCode(string) string { return "registration_failed" }

func boolPtr(value bool) *bool { return &value }
