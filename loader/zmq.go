package loader

import (
	"blkparser/utils"
	"encoding/hex"
	"log"

	"github.com/zeromq/goczmq"
)

func ZmqNotify(endpoint string, block chan string) {

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
