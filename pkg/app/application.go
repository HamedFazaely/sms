package app

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"gitlab.com/Hamed1984/sms/pkg/conf"
	"gitlab.com/Hamed1984/sms/pkg/db"
	"gitlab.com/Hamed1984/sms/pkg/logging"
	"gitlab.com/Hamed1984/sms/pkg/mq"
	"gitlab.com/Hamed1984/sms/pkg/smssender"
	"go.uber.org/zap"
)

type Application struct {
	config      *conf.Configuration
	done        chan struct{}
	smsRepo     db.SMSRepo
	smsMq       mq.SMSPublisherConsumer
	smsSender   smssender.SMSSender
	smsPubch    chan *mq.SMSMsg
	smsPubErrch chan *mq.SMSPubError
}

func NewApplication(c *conf.Configuration) (*Application, error) {

	dbConn, err := db.NewMysqlConnection(c)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	smsPch := make(chan *mq.SMSMsg)
	smsPerrCh := make(chan *mq.SMSPubError)

	repo := db.NewSMSRepoImpl(c, dbConn)
	smsSender := smssender.NewMockSMSSender(c)

	msgMQ, err := mq.NewSMS(c, done)
	if err != nil {
		return nil, err
	}

	go func() {

		for x := range smsPerrCh {
			logging.GetLogger(c).Error(x.Err.Error())
			logging.GetLogger(c).Info("retry publishing message", zap.Uint64("order_id", x.Msg.OrderID))
			smsPch <- x.Msg
		}

	}()

	msgMQ.StartPublisher(smsPch, smsPerrCh)

	msgMQ.StartConsume(func(d amqp.Delivery, done <-chan struct{}) error {
		select {
		case <-done:
			return nil
		default:
			var msg mq.SMSMsg
			err := json.Unmarshal(d.Body, &msg)
			if err != nil {
				logging.GetLogger(c).Error(err.Error())
				d.Ack(false)
				return nil
			}
			logging.GetLogger(c).Info("message received", zap.Any("sms", msg))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = repo.CreateMessage(ctx, &db.SMSMessage{
				OrderID:     msg.OrderID,
				PhoneNumner: msg.PhoneNumber,
			})
			if err != nil {
				if err == db.ErrMessageAlreadyReceived {
					logging.GetLogger(c).Info("duplicate message", zap.Any("sms", msg))
					d.Ack(false)
					return nil
				}
				logging.GetLogger(c).Error(err.Error())
				d.Nack(false, true)
				return nil
			}
			err = smsSender.SendSMS(context.Background(), msg.PhoneNumber, "Hello")
			if err != nil {
				logging.GetLogger(c).Error(err.Error())
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err = repo.IncrementRetryCount(ctx, msg.OrderID)
				if err != nil {
					if err == db.ErrRetryCountAlreadyIncremented {
						d.Nack(false, true)
						return nil
					}
					if err == db.ErrMaxRetryCountReached {
						d.Nack(false, false)
						return nil
					}
					d.Nack(false, true)
					return nil
				}
				d.Nack(false, true)
				return nil
			}
			d.Ack(false)
			go func() {
				m := mq.SMSMsg{
					OrderID:     msg.OrderID,
					PhoneNumber: msg.PhoneNumber,
					CreatedAt:   time.Now(),
				}
				select {
				case <-done:
					return
				case smsPch <- &m:
					return
				}
			}()

		}
		return nil

	})

	ret := &Application{
		config:      c,
		done:        done,
		smsRepo:     repo,
		smsMq:       msgMQ,
		smsSender:   smsSender,
		smsPubch:    smsPch,
		smsPubErrch: smsPerrCh,
	}
	return ret, nil

}

func (a *Application) Start() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	close(a.done)
}
