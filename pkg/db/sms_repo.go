package db

import (
	"context"
	"database/sql"
	"errors"
	"log"

	"github.com/go-sql-driver/mysql"
	"gitlab.com/Hamed1984/sms/pkg/conf"
)

type SMSRepo interface {
	CreateMessage(cxt context.Context, m *SMSMessage) (*SMSMessage, error)
	IncrementRetryCount(ctx context.Context, id uint64) error
}

type SMSRepoImpl struct {
	config *conf.Configuration
	db     *sql.DB
}

func NewSMSRepoImpl(c *conf.Configuration, db *sql.DB) *SMSRepoImpl {
	return &SMSRepoImpl{
		config: c,
		db:     db,
	}
}

func (s *SMSRepoImpl) CreateMessage(ctx context.Context, m *SMSMessage) (*SMSMessage, error) {
	_, err := s.db.ExecContext(ctx, "INSERT INTO messages (order_id,phone_number) VALUES (?,?)", m.OrderID, m.PhoneNumner)
	if err != nil {
		if mysqlErr, ok := err.(*mysql.MySQLError); ok {
			if mysqlErr.Number == 1062 {
				return nil, ErrMessageAlreadyReceived
			}
		}
		return nil, err
	}
	return m, nil
}

func (s *SMSRepoImpl) IncrementRetryCount(ctx context.Context, id uint64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer s.rollBack(tx)

	msg, err := s.getMessageByIDTxn(ctx, tx, id)
	if err != nil {
		return err
	}
	if msg.RetryCount >= s.config.MaxRetryCount {
		return ErrMaxRetryCountReached
	}
	res, err := tx.ExecContext(ctx, "UPDATE messages SET retry_count = ? WHERE order_id = ? AND updated_at = ?", msg.RetryCount+1, id, msg.UpdatedAt)
	if err != nil {
		return err
	}
	ra, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if ra == 0 {
		return ErrRetryCountAlreadyIncremented
	}
	err = tx.Commit()
	return err
}

func (o *SMSRepoImpl) getMessageByIDTxn(ctx context.Context, tx *sql.Tx, id uint64) (*SMSMessage, error) {
	var msg SMSMessage
	row := tx.QueryRowContext(ctx, "SELECT * FROM messages WHERE order_id = ? ", id)
	err := row.Scan(&msg.OrderID, &msg.PhoneNumner, &msg.RetryCount, &msg.CreatedAt, &msg.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (o *SMSRepoImpl) rollBack(tx *sql.Tx) {
	err := tx.Rollback()
	if err != nil {
		if !errors.Is(err, sql.ErrTxDone) {
			log.Printf("an error ocuured during transaxtion rollback: %s\n", err.Error())
		}
	}

}
