package loader

import (
	"blkparser/utils"
	"encoding/hex"
	"log"

	"github.com/zeromq/goczmq"
)

func ZmqNotify(block chan string) {
	endpoint := "tcp://192.168.31.236:16331"

	subscriber, err := goczmq.NewSub(endpoint, "hashblock")
	defer subscriber.Destroy()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("ZeroMQ started to listen for blocks")

	for {
		msg, _, err := subscriber.RecvFrame()
		if err != nil {
			log.Printf("Error ZMQ RecFrame: %s", err)
		}

		if len(msg) == 32 {
			block <- utils.HashString(msg)
		}
		log.Printf("received '%s'", hex.EncodeToString(msg))
	}
}
