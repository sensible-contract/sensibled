package parser

import (
	"blkparser/loader"
	"blkparser/model"
	"blkparser/task"
	serialTask "blkparser/task/serial"
	"blkparser/utils"
	"log"
	"sync"
)

type Blockchain struct {
	Blocks        map[string]*model.Block // 所有区块
	BlocksOfChain map[string]*model.Block // 主链区块
	MaxBlock      *model.Block
	GenesisBlock  *model.Block
	BlockData     *loader.BlockData
	m             sync.Mutex
}

func NewBlockchain(path string, magic [4]byte) (bc *Blockchain, err error) {
	bc = new(Blockchain)
	bc.Blocks = make(map[string]*model.Block, 0)
	bc.BlocksOfChain = make(map[string]*model.Block, 0)

	bc.BlockData, err = loader.NewBlockData(path, magic)
	if err != nil {
		return nil, err
	}
	return
}

func (bc *Blockchain) ParseLongestChain(startBlockHeight, endBlockHeight int) {
	bc.InitLongestChainHeader()

	if ok := bc.SkipToBlockFileIdByBlockHeight(startBlockHeight); !ok {
		log.Printf("skip to block height failed: %d", startBlockHeight)
		return
	}

	blocksReady := make(chan *model.Block, 256)

	go bc.InitLongestChainBlock(blocksReady, startBlockHeight, endBlockHeight)

	bc.ParseLongestChainBlock(blocksReady, startBlockHeight, endBlockHeight)

	// 最后分析执行
	task.ParseEnd()
}

// InitLongestChainBlock 解码区块，生产者
func (bc *Blockchain) InitLongestChainBlock(blocksReady chan *model.Block, startBlockHeight, endBlockHeight int) {
	var wg sync.WaitGroup
	parsers := make(chan struct{}, 32)
	for {
		// 获取所有Block字节，不要求有序返回或属于主链
		// 但由于未分析的区块需要暂存，无序遍历会增加内存消耗
		rawblock, err := bc.BlockData.GetRawBlock()
		if err != nil {
			log.Printf("get block error: %v", err)
			break
		}
		if len(rawblock) < 80 {
			continue
		}
		blockHash := utils.HashString(utils.GetShaString(rawblock[:80]))
		block, ok := bc.BlocksOfChain[blockHash]
		if !ok {
			// 若不是主链区块则跳过
			continue
		}
		if block.Height < startBlockHeight {
			continue
		}

		block.Raw = rawblock
		block.Size = uint32(len(rawblock))

		parsers <- struct{}{}
		wg.Add(1)
		go func(block *model.Block) {
			defer wg.Done()

			// 先并行分析交易
			processBlock := &model.ProcessBlock{
				Height:         block.Height,
				UtxoMap:        make(map[string]model.CalcData, 1),
				UtxoMissingMap: make(map[string]bool, 1),
			}
			txs := NewTxs(block.Raw[80:])
			for idx, tx := range txs {
				isCoinbase := (idx == 0)
				task.ParseTxParallel(tx, isCoinbase, processBlock)
			}

			block.ParseData = processBlock
			block.Txs = txs
			block.Raw = nil

			blocksReady <- block

			<-parsers
		}(block)
	}
	wg.Wait()

	close(blocksReady)
}

// ParseLongestChainBlock 按顺序消费解码后的区块
func (bc *Blockchain) ParseLongestChainBlock(blocksReady chan *model.Block, nextBlockHeight, maxBlockHeight int) {
	blocksTotal := len(bc.BlocksOfChain)
	if maxBlockHeight < 0 || maxBlockHeight > blocksTotal {
		maxBlockHeight = blocksTotal
	}
	blockParseBufferBlock := make([]*model.Block, maxBlockHeight)
	for block := range blocksReady {
		// 暂存block
		if block.Height < maxBlockHeight {
			blockParseBufferBlock[block.Height] = block
		}
		// 按序
		if block.Height != nextBlockHeight {
			continue
		}
		for nextBlockHeight < maxBlockHeight {
			block = blockParseBufferBlock[nextBlockHeight]
			if block == nil { // 检查是否准备好
				break
			}

			// 再串行分析区块
			task.ParseBlockSerial(block, maxBlockHeight)

			block.Txs = nil
			nextBlockHeight++
		}
		if nextBlockHeight >= maxBlockHeight {
			break
		}
	}
}

func (bc *Blockchain) SkipToBlockFileIdByBlockHeight(height int) bool {
	for idx := 0; ; idx++ {
		if err := bc.BlockData.SkipTo(idx, 0); err != nil {
			skipTo := 0
			if idx > 2 {
				skipTo = idx - 2
			}
			bc.BlockData.SkipTo(skipTo, 0)
			return true
		}

		for {
			rawblock, err := bc.BlockData.GetRawBlockHeader()
			if err != nil {
				break
			}
			block := NewBlock(rawblock)
			blockInfo, ok := bc.BlocksOfChain[block.HashHex]
			if !ok {
				continue
			}
			if blockInfo.Height < height {
				break
			}
			skipTo := 0
			if idx > 2 {
				skipTo = idx - 2
			}
			bc.BlockData.SkipTo(skipTo, 0)
			return true
		}
	}
	return false
}

func (bc *Blockchain) InitLongestChainHeader() {
	bc.LoadAllBlockHeaders(true)
	bc.BlockData.HeaderFile.Close()

	idx := 0
	for ; ; idx++ {
		err := bc.BlockData.SkipTo(idx, 0)
		if err != nil {
			break
		}
		rawblock, err := bc.BlockData.GetRawBlockHeader()
		if err != nil {
			break
		}
		block := NewBlock(rawblock)
		if _, ok := bc.Blocks[block.HashHex]; !ok {
			break
		}
	}

	skipTo := 0
	if idx > 0 {
		skipTo = idx - 1
	}
	if err := bc.BlockData.SkipTo(skipTo, 0); err == nil {
		bc.LoadAllBlockHeaders(false)
		bc.BlockData.HeaderFileWriter.Flush()
	}
	bc.BlockData.HeaderFileA.Close()

	bc.SetBlockHeight()
	bc.SelectLongestChain()
}

func (bc *Blockchain) LoadAllBlockHeaders(loadCacheHeader bool) {
	// 读取所有的rawBlock
	parsers := make(chan struct{}, 30)
	var wg sync.WaitGroup
	for idx := 0; ; idx++ {
		// 获取所有Block Header字节，不要求有序返回或属于主链
		var rawblock []byte
		var err error
		if loadCacheHeader {
			rawblock, err = bc.BlockData.GetCacheRawBlockHeader()
		} else {
			rawblock, err = bc.BlockData.GetRawBlockHeader()
		}
		if err != nil {
			// log.Printf("no more block header: %v", err)
			break
		}
		if len(rawblock) < 80 {
			continue
		}

		parsers <- struct{}{}
		wg.Add(1)
		go func(rawblock []byte) {
			defer wg.Done()
			block := NewBlock(rawblock)

			bc.m.Lock()
			if _, ok := bc.Blocks[block.HashHex]; !ok {
				bc.Blocks[block.HashHex] = block
				if !loadCacheHeader {
					bc.BlockData.SetCacheRawBlockHeader(rawblock)
				}
			}
			bc.m.Unlock()

			<-parsers
		}(rawblock)

		// header speed
		serialTask.ParseBlockSpeed(0, idx, 0)
	}
	wg.Wait()
}

func (bc *Blockchain) SetBlockHeight() {
	log.Printf("plain blocks count: %d", len(bc.Blocks))
	// 设置所有区块的高度，包括分支链的高度
	for _, block := range bc.Blocks {
		// 已设置区块高度则跳过
		if block.Height > 0 {
			continue
		}
		// 未设置则检查parent block的高度
		thisBlock := block
		for {
			parentBlock, ok := bc.Blocks[thisBlock.ParentHex]
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
			nextBlock, ok := bc.Blocks[currBlock.NextHex]
			if !ok {
				break
			}
			nextBlock.Height = currBlock.Height + 1
			currBlock = nextBlock
		}
		// 记录maxBlock
		if bc.MaxBlock == nil || currBlock.Height > bc.MaxBlock.Height {
			bc.MaxBlock = currBlock
		}
	}

}

func (bc *Blockchain) SelectLongestChain() {
	// 提取最长主链
	block := bc.MaxBlock
	for {
		// 由于之前的高度是从1开始，现在统一减一
		block.Height -= 1
		bc.BlocksOfChain[block.HashHex] = block
		// 设置genesis
		bc.GenesisBlock = block
		var ok bool
		block, ok = bc.Blocks[block.ParentHex]
		if !ok {
			break
		}
	}
	log.Printf("genesis block: %s", bc.GenesisBlock.HashHex)
	log.Printf("chain blocks count: %d", len(bc.BlocksOfChain))
}
