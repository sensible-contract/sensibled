package blkparser

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

type Blockchain struct {
	Path          string
	Magic         [4]byte
	CurrentFile   *os.File
	HeaderFile    *os.File
	CurrentId     int
	Offset        int
	Blocks        map[string]*Block // 所有区块
	BlocksOfChain map[string]*Block // 主链区块
	MaxBlock      *Block
	GenesisBlock  *Block
	m             sync.Mutex
}

func makeFileRandomRead(f *os.File) (err error) {
	fi, err := f.Stat()
	if err != nil {
		return err
	}

	err = unix.Fadvise(int(f.Fd()), 0, fi.Size(), unix.FADV_RANDOM)
	if err != nil {
		return fmt.Errorf("failed to purge page cache")
	}
	return nil
}

func NewBlockchain(path string, magic [4]byte) (blockchain *Blockchain, err error) {
	blockchain = new(Blockchain)
	blockchain.Path = path
	blockchain.Magic = magic
	blockchain.CurrentId = 0
	blockchain.Offset = 0
	blockchain.Blocks = make(map[string]*Block, 0)
	blockchain.BlocksOfChain = make(map[string]*Block, 0)
	f, err := os.Open(blkfilename(path, 0))
	if err != nil {
		return
	}

	blockchain.CurrentFile = f

	// headerFile, err := os.Open(path + "/block_header_cache.dat")
	// if err != nil {
	// 	return
	// }
	// blockchain.HeaderFile = headerFile
	return
}

func (blockchain *Blockchain) ParseLongestChain() {
	doneParse := make(chan struct{}, 0)
	blocksReady := make(chan *Block, 32)
	// 按顺序消费解码后的区块
	go func() {
		nextBlockHeight := 0
		maxBlockHeight := len(blockchain.BlocksOfChain)
		blockParseBufferBlock := make([]*Block, maxBlockHeight)
		for block := range blocksReady {
			// 暂存block
			blockParseBufferBlock[block.Height] = block
			// 按序
			if block.Height != nextBlockHeight {
				continue
			}
			for nextBlockHeight < maxBlockHeight {
				block = blockParseBufferBlock[nextBlockHeight]
				if block == nil { // 检查是否准备好
					break
				}

				// 分析区块
				ParseBlock(block)

				block.Txs = nil
				nextBlockHeight++
			}
		}

		ParseEnd()

		doneParse <- struct{}{}
	}()

	// 解码区块，生产者
	var wg sync.WaitGroup
	parsers := make(chan struct{}, 30)
	for {
		fileIdx, offset, rawblock, err := blockchain.NextRawBlock(false)
		if err != nil {
			log.Printf("get block error: %v", err)
			break
		}
		if len(rawblock) < 80 {
			continue
		}
		blockHash := HashString(GetShaString(rawblock[:80]))
		block, ok := blockchain.BlocksOfChain[blockHash]
		if !ok {
			// 若不是主链区块则跳过
			continue
		}

		block.Raw = rawblock
		block.FileIdx = fileIdx
		block.FileOffset = offset
		block.Size = uint32(len(rawblock))

		parsers <- struct{}{}
		wg.Add(1)
		go func(block *Block) {
			defer wg.Done()

			txs, _ := ParseTxs(block.Raw[80:])
			block.Txs = txs
			block.Raw = nil

			blocksReady <- block

			<-parsers
		}(block)
	}
	wg.Wait()

	close(blocksReady)

	<-doneParse
}

func (blockchain *Blockchain) InitLongestChain() {
	// 读取所有的rawBlock
	parsers := make(chan struct{}, 30)
	var wg sync.WaitGroup
	for idx := 0; ; idx++ {
		fileIdx, offset, rawblock, err := blockchain.NextRawBlock(true)
		if err != nil {
			log.Printf("get block error: %v", err)
			break
		}
		if len(rawblock) < 80 {
			continue
		}

		parsers <- struct{}{}
		wg.Add(1)
		go func(fileIdx, offset int, rawblock []byte) {
			defer wg.Done()
			block, err := NewBlock(rawblock)
			if err != nil {
				return
			}
			blockchain.m.Lock()
			blockchain.Blocks[block.HashHex] = block
			blockchain.m.Unlock()
			<-parsers
		}(fileIdx, offset, rawblock)

		ParseBlockSpeed(0, idx)
	}
	wg.Wait()

	log.Printf("plain blocks count: %d", len(blockchain.Blocks))
	// 设置所有区块的高度，包括分支链的高度
	for _, block := range blockchain.Blocks {
		// 已设置区块高度则跳过
		if block.Height > 0 {
			continue
		}
		// 未设置则检查parent block的高度
		thisBlock := block
		for {
			parentBlock, ok := blockchain.Blocks[thisBlock.ParentHex]
			// 如果找不到parent block，则初始化此block高度为1，后续处理时将统一减一
			if !ok {
				thisBlock.Height = 1
				break
			}
			// 如果能找到parent block，先串联block
			parentBlock.NextHex = thisBlock.HashHex
			// 若parent block 高度未设置，则继续看前面的Parent block
			if parentBlock.Height <= 0 {
				thisBlock = parentBlock
				continue
			}
			// 若parent block 高度已设置，则更新当前block高度
			thisBlock.Height = parentBlock.Height + 1
			break
		}
		// 依次更新后续block高度
		currBlock := thisBlock
		for {
			nextBlock, ok := blockchain.Blocks[currBlock.NextHex]
			if !ok {
				break
			}
			nextBlock.Height = currBlock.Height + 1
			currBlock = nextBlock
		}
		// 记录maxBlock
		if blockchain.MaxBlock == nil || currBlock.Height > blockchain.MaxBlock.Height {
			blockchain.MaxBlock = currBlock
		}
	}
	// 提取最长主链
	block := blockchain.MaxBlock
	for {
		// 由于之前的高度是从1开始，现在统一减一
		block.Height -= 1
		blockchain.BlocksOfChain[block.HashHex] = block
		// 设置genesis
		blockchain.GenesisBlock = block
		var ok bool
		block, ok = blockchain.Blocks[block.ParentHex]
		if !ok {
			break
		}
	}
	log.Printf("genesis block: %s", blockchain.GenesisBlock.HashHex)
	log.Printf("chain blocks count: %d", len(blockchain.BlocksOfChain))
}

func (blockchain *Blockchain) NextRawBlock(skipTxs bool) (fileIdx, offset int, rawblock []byte, err error) {
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
		// log.Printf("read failed: %v", err)
		return
	}
	blockchain.Offset += 4

	if !bytes.Equal(buf[:], blockchain.Magic[:]) {
		err = errors.New("Bad magic")
		log.Printf("read blk%d[%d] failed: %v, %v != %v", blockchain.CurrentId, blockchain.Offset, err, buf[:], blockchain.Magic[:])
		return
	}

	_, err = blockchain.CurrentFile.Read(buf[:])
	if err != nil {
		return
	}
	blockchain.Offset += 4

	blocksize := binary.LittleEndian.Uint32(buf[:])

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
