package blkparser

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"
)

var (
	lastLogTime      time.Time
	lastBlockHeight  int
	lastBlockTxCount int

	utxo map[string]int
)

func init() {
	utxo = make(map[string]int, 0)
}

func ParseBlock(block *Block) {
	ParseBlockSpeed(len(block.Txs), block.Height)
	// ParseBlockCount(block)

	// dumpBlock(block)
	// dumpBlockTx(block)

	dumpUtxo(block)
	dumpTxoSpendBy(block)

	block.Txs = nil
}

func ParseEnd() {
	filePathUTXO := "/data/utxo.bsv"
	fileUTXO, err := os.OpenFile(filePathUTXO, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return
	}
	defer fileUTXO.Close()

	write := bufio.NewWriter(fileUTXO)

	log.Printf("len utxo: %d", len(utxo))
	for keyStr, value := range utxo {
		key := []byte(keyStr)

		write.WriteString(fmt.Sprintf("%s %d %d\n",
			HashString(key[:32]),
			binary.LittleEndian.Uint32(key[32:]), value))
	}

	write.Flush()
}

func ParseBlockSpeed(nTx int, nextBlockHeight int) {
	lastBlockTxCount += nTx

	if time.Since(lastLogTime) > time.Second {
		if nextBlockHeight < lastBlockHeight {
			lastBlockHeight = 0
		}

		lastLogTime = time.Now()
		log.Printf("%d, speed: %d bps, tx: %d tps, utxo: %d",
			nextBlockHeight,
			nextBlockHeight-lastBlockHeight,
			lastBlockTxCount,
			len(utxo),
		)
		lastBlockHeight = nextBlockHeight
		lastBlockTxCount = 0
	}
}

func ParseBlockCount(block *Block) {
	txs := block.Txs

	// 检查一些统计项
	countInsideTx := CheckTxsOrder(txs)
	countWitTx := CountWitTxsInBlock(txs)
	countValueTx := CountValueOfTxsInBlock(txs)
	countZeroValueTx := CountZeroValueOfTxsInBlock(txs)

	log.Printf("%d Time: %d blk: %s size: %d nTx: %d %d %d %d value: %d",
		block.Height,
		block.BlockTime,
		block.HashHex,
		block.Size, len(txs),
		countInsideTx, countWitTx, countZeroValueTx,
		countValueTx,
	)

}

// dumpBlock block id
func dumpBlock(block *Block) {
	fmt.Printf("blkid %s %d\n",
		block.HashHex,
		block.Height,
	)
}

// dumpBlockTx all tx in block height
func dumpBlockTx(block *Block) {
	for _, tx := range block.Txs {
		fmt.Printf("tx-of-block %s %d\n",
			tx.HashHex,
			block.Height,
		)
	}
}

// dumpUtxo utxo 信息
func dumpUtxo(block *Block) {
	txs := block.Txs

	for _, tx := range txs {
		for idx, output := range tx.TxOuts {
			if output.Value == 0 || !output.LockingScriptMatch {
				continue
			}

			utxo[output.OutpointKey] = int(output.Value)

			fmt.Printf("utxo %s %d %d\n",
				tx.HashHex,
				idx,
				output.Value,
			)
		}
	}
}

// dumpTxoSpendBy utxo被使用
func dumpTxoSpendBy(block *Block) {
	txs := block.Txs
	for _, tx := range txs[1:] {
		for idx, input := range tx.TxIns {
			if _, ok := utxo[input.InputOutpointKey]; !ok {
				continue
			}
			delete(utxo, input.InputOutpointKey)

			fmt.Printf("spend %s %d %s %d\n",
				input.InputHashHex,
				input.InputVout,
				tx.HashHex,
				idx,
			)
		}
	}
}
