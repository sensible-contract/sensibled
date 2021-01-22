package main

import (
	blkparser "blkparser/lib"
	"log"
	// _ "net/http/pprof"
)

func main() {
	// f, err := os.Create("cpu.prof")
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	blockchain, err := blkparser.NewBlockchain(
		// "./blocks", [4]byte{0xf9, 0xbe, 0xb4, 0xd9}) // bitcoin
		"./blocks-bsv", [4]byte{0xf9, 0xbe, 0xb4, 0xd9}) // bitcoin-sv
	// "/data/bitcoin-sv-blocks/blocks", [4]byte{0xf9, 0xbe, 0xb4, 0xd9}) // bitcoin-sv
	// "./blocks-bsv-test", [4]byte{0x0b, 0x11, 0x09, 0x07}) // bsv-test

	if err != nil {
		log.Printf("init chain error: %v", err)
		return
	}

	// go func() {
	blockchain.InitLongestChain()
	blockchain.SkipTo(0, 0)
	blockchain.ParseLongestChain()
	log.Printf("finished")
	// }()

	// go tool pprof http://localhost:8080/debug/pprof/profile
	// http.ListenAndServe("0.0.0.0:8080", nil)
}
