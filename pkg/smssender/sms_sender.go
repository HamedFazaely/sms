package smssender

import (
	"context"

	"gitlab.com/Hamed1984/sms/pkg/conf"
	"gitlab.com/Hamed1984/sms/pkg/logging"
	"go.uber.org/zap"
)

type SMSSender interface {
	SendSMS(ctx context.Context, phone string, body string) error
}

type MockSMSSender struct {
	config *conf.Configuration
}

func NewMockSMSSender(c *conf.Configuration) *MockSMSSender {
	return &MockSMSSender{c}
}

func (m *MockSMSSender) SendSMS(ctx context.Context, phone string, body string) error {
	logging.GetLogger(m.config).Info("sms sent", zap.String("phone", phone))
	return nil
}
