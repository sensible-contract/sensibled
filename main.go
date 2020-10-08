package main

import (
	blkparser "blkparser/lib"
	"log"
	_ "net/http/pprof"
)

func main() {
	// f, err := os.Create("cpu.prof")
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	blockchain, err := blkparser.NewBlockchain(
		"./blocks", // bitcoin
		// "/data/bitcoin-sv-blocks/blocks", // bitcoin-sv
		[4]byte{0xf9, 0xbe, 0xb4, 0xd9})
	if err != nil {
		log.Printf("init chain error: %v", err)
		return
	}

	blockchain.InitLongestChain()

	// go func() {
	// 	// blockchain.SkipTo(973, 0)
	// 	blockchain.InitLongestChain()
	// }()

	// http.ListenAndServe("0.0.0.0:8080", nil)
}
