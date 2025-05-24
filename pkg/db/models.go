package db

import (
	"errors"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var ErrMaxRetryCountReached = errors.New("max retry count reached")
var ErrRetryCountAlreadyIncremented = errors.New("retry count already incremented")
var ErrMessageAlreadyReceived = errors.New("message already received")

type SMSMessage struct {
	OrderID     uint64
	PhoneNumner string
	RetryCount  int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
