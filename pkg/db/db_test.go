package db

import (
	"context"
	"testing"

	"gitlab.com/Hamed1984/sms/pkg/conf"
)

func TestCreateMessage(t *testing.T) {
	t.Setenv("DB_PASSWORD", "hamed1984")
	t.Setenv("MQ_PASSWORD", "hamed1984")
	conf := conf.GetConffiguration()
	db, err := NewMysqlConnection(conf)
	if err != nil {
		t.Fatal(err)
	}

	repo := NewSMSRepoImpl(conf, db)
	msg := &SMSMessage{
		OrderID:     1,
		PhoneNumner: "9197459057",
	}
	_, err = repo.CreateMessage(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIncrementRetryCount(t *testing.T) {
	t.Setenv("DB_PASSWORD", "hamed1984")
	t.Setenv("MQ_PASSWORD", "hamed1984")
	conf := conf.GetConffiguration()
	db, err := NewMysqlConnection(conf)
	if err != nil {
		t.Fatal(err)
	}

	repo := NewSMSRepoImpl(conf, db)
	err = repo.IncrementRetryCount(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
}
