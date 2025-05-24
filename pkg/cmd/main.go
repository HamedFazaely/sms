package main

import (
	"log"

	"gitlab.com/Hamed1984/sms/pkg/app"
	"gitlab.com/Hamed1984/sms/pkg/conf"
)

func main() {
	app, err := app.NewApplication(conf.GetConffiguration())
	if err != nil {
		log.Fatal(err)
	}
	app.Start()
}
