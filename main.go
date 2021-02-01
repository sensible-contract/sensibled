package main

import (
	"blkparser/parser"
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"
)

var startBlockHeight, endBlockHeight int

func init() {
	flag.IntVar(&startBlockHeight, "start", 0, "start block height")
	flag.IntVar(&endBlockHeight, "end", -1, "end block height")
	flag.Parse()
}

func main() {
	blockchain, err := parser.NewBlockchain(
		"./blocks-bsv", [4]byte{0xf9, 0xbe, 0xb4, 0xd9}) // bitcoin-sv
	// "./blocks", [4]byte{0xf9, 0xbe, 0xb4, 0xd9}) // bitcoin
	// "/data/bitcoin-sv-blocks/blocks", [4]byte{0xf9, 0xbe, 0xb4, 0xd9}) // bitcoin-sv
	// "./blocks-bsv-test", [4]byte{0x0b, 0x11, 0x09, 0x07}) // bsv-test

	if err != nil {
		log.Printf("init chain error: %v", err)
		return
	}

	server := &http.Server{Addr: "0.0.0.0:8080", Handler: nil}
	go func() {

		blockchain.ParseLongestChain(startBlockHeight, endBlockHeight)
		log.Printf("finished")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()

	// go tool pprof http://localhost:8080/debug/pprof/profile
	server.ListenAndServe()
}
