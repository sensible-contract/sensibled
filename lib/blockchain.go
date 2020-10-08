package blkparser

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type Blockchain struct {
	Path        string
	Magic       [4]byte
	CurrentFile *os.File
	CurrentId   int
	Offset      int
	Blocks      map[string]*Block
	MaxBlock    *Block
	m           sync.Mutex
}

func NewBlockchain(path string, magic [4]byte) (blockchain *Blockchain, err error) {
	blockchain = new(Blockchain)
	blockchain.Path = path
	blockchain.Magic = magic
	blockchain.CurrentId = 0
	blockchain.Offset = 0
	blockchain.Blocks = make(map[string]*Block, 0)

	f, err := os.Open(blkfilename(path, 0))
	if err != nil {
		return
	}

	blockchain.CurrentFile = f
	return
}

func (blockchain *Blockchain) InitLongestChain() {
	now := time.Now()
	pool := make(chan struct{}, 24)
	var wg sync.WaitGroup
	for idx := range make([]struct{}, 660000) {
		nblk, offset, rawblock, err := blockchain.NextRawBlock(true)
		if err != nil {
			log.Printf("get block error: %v", err)
			break
		}
		if len(rawblock) < 80 {
			continue
		}

		pool <- struct{}{}
		wg.Add(1)
		go func(nblk, offset, idx int, rawblock []byte) {
			defer wg.Done()

			block, err := NewBlock(rawblock)
			if err != nil {
				return
			}

			blockchain.m.Lock()
			blockchain.Blocks[block.Hash] = block
			blockchain.m.Unlock()

			// log.Printf("%d[%d]: %d, id: %s, %s, size: %d, ntx: %d, time: %d", nblk, offset, idx,
			// 	block.Hash, block.Parent,
			// 	block.Size, len(block.Txs), block.BlockTime,
			// )
			<-pool
		}(nblk, offset, idx, rawblock)

		if time.Since(now) > time.Second {
			now = time.Now()
			// log.Printf("block %d, size %d, id %s", idx, block.Size,
			// 	blkparser.HashString(blkparser.GetShaString(block.Hash)))
		}

		// blockJson, err := json.Marshal(block)
		// if err != nil {
		// 	log.Printf("marshal block failed: %v", err)
		// 	return
		// }
		// log.Printf("block: %s", blockJson)
	}
	wg.Wait()

	genesisBlock := blockchain.Blocks["000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"]
	genesisBlock.Height = 1
	blockchain.MaxBlock = genesisBlock

	log.Printf("blocks count: %d", len(blockchain.Blocks))
	for _, block := range blockchain.Blocks {
		if block.Height > 0 {
			continue
		}
		for block.Parent != "" {
			parentBlock, ok := blockchain.Blocks[block.Parent]
			if !ok {
				block.Height = 1
				for block.Next != "" {
					nextBlock, ok := blockchain.Blocks[block.Next]
					if !ok {
						break
					}
					nextBlock.Height = block.Height + 1
					block = nextBlock
				}
				break
			}

			parentBlock.Next = block.Hash
			if parentBlock.Height > 0 {
				block.Height = parentBlock.Height + 1
				if block.Height > blockchain.MaxBlock.Height {
					blockchain.MaxBlock = block
				}
				for block.Next != "" {
					nextBlock, ok := blockchain.Blocks[block.Next]
					if !ok {
						break
					}
					nextBlock.Height = block.Height + 1
					if nextBlock.Height > blockchain.MaxBlock.Height {
						blockchain.MaxBlock = nextBlock
					}
					block = nextBlock
				}
				break
			}

			block = parentBlock
		}
	}

	block := blockchain.MaxBlock
	for {
		log.Printf("%d, id: %s, size: %d, time: %d", block.Height,
			block.Hash,
			block.Size, block.BlockTime,
		)
		var ok bool
		block, ok = blockchain.Blocks[block.Parent]
		if !ok {
			break
		}
	}
}

func (blockchain *Blockchain) NextRawBlock(skipTxs bool) (nblk, offset int, rawblock []byte, err error) {
	blockchain.m.Lock()
	defer blockchain.m.Unlock()

	rawblock, err = blockchain.FetchNextBlock(skipTxs)
	if err != nil {
		newblkfile, err2 := os.Open(blkfilename(blockchain.Path, blockchain.CurrentId+1))
		if err2 != nil {
			return blockchain.CurrentId, 0, nil, err2
		}
		blockchain.CurrentId++
		blockchain.CurrentFile.Close()
		blockchain.CurrentFile = newblkfile
		blockchain.Offset = 0
		rawblock, err = blockchain.FetchNextBlock(skipTxs)
	}
	return blockchain.CurrentId, blockchain.Offset, rawblock, nil
}

func (blockchain *Blockchain) FetchNextBlock(skipTxs bool) (rawblock []byte, err error) {
	buf := [4]byte{}
	_, err = blockchain.CurrentFile.Read(buf[:])
	if err != nil {
		log.Printf("read failed: %v", err)
		return
	}
	blockchain.Offset += 4

	if !bytes.Equal(buf[:], blockchain.Magic[:]) {
		err = errors.New("Bad magic")
		log.Printf("read blk%d[%d] failed: %v, %v != %v", blockchain.CurrentId, blockchain.Offset, err, buf[:], blockchain.Magic[:])
		// return
	}

	_, err = blockchain.CurrentFile.Read(buf[:])
	if err != nil {
		return
	}
	blockchain.Offset += 4

	blocksize := binary.LittleEndian.Uint32(buf[:])

	// fix-bsv-bug: block 77
	var isBsv bool = false
	if isBsv && blocksize == 74416 && blockchain.CurrentId == 77 {
		log.Printf("skip in block 77")
		return
	}

	// log.Printf("blocksize: %d", blocksize)
	if skipTxs {
		rawblock = make([]byte, 80)
	} else {
		rawblock = make([]byte, blocksize)
	}
	_, err = blockchain.CurrentFile.Read(rawblock[:])
	if err != nil {
		return
	}

	if skipTxs {
		_, err = blockchain.CurrentFile.Seek(int64(blocksize-80), os.SEEK_CUR)
		if err != nil {
			return
		}
	}
	blockchain.Offset += int(blocksize)
	return
}

// Convenience method to skip directly to the given blkfile / offset,
// you must take care of the height
func (blockchain *Blockchain) SkipTo(blkId int, offset int64) (err error) {
	blockchain.m.Lock()
	defer blockchain.m.Unlock()

	blockchain.CurrentId = blkId
	blockchain.Offset = 0
	f, err := os.Open(blkfilename(blockchain.Path, blkId))
	if err != nil {
		return
	}
	blockchain.CurrentFile.Close()
	blockchain.CurrentFile = f
	_, err = blockchain.CurrentFile.Seek(offset, 0)
	blockchain.Offset = int(offset)
	return
}

func blkfilename(path string, id int) string {
	return fmt.Sprintf("%s/blk%05d.dat", path, id)
}
