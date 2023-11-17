package model

import (
	"encoding/binary"
	"sync"

	scriptDecoder "github.com/sensible-contract/sensible-script-decoder"
	"go.uber.org/multierr"
	"go.uber.org/zap/zapcore"
)

const MEMPOOL_HEIGHT = 4294967295

var FALSE_OP_RETURN []byte = []byte("\x00\x6a")

type Tx struct {
	Raw          []byte
	TxIdHex      string // 64
	TxId         []byte // 32
	Size         uint32
	LockTime     uint32
	Version      uint32
	TxInCnt      uint32
	TxOutCnt     uint32
	InputsValue  uint64
	OutputsValue uint64
	TxIns        TxIns
	TxOuts       TxOuts
	IsSensible   bool
}

type TxIn struct {
	InputHashHex string // 32
	InputHash    []byte // 32
	InputVout    uint32
	ScriptSig    []byte
	Sequence     uint32

	// other:
	InputOutpointKey string // 32 + 4
	InputOutpoint    []byte // 32 + 4
	InputPoint       []byte // 32 + 4
}

func (t *TxIn) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("t", t.InputHashHex)
	enc.AddUint32("i", t.InputVout)
	return nil
}

type TxOut struct {
	Satoshi  uint64
	PkScript []byte

	OutpointIdxKey string // 4
	ScriptType     []byte

	AddressData *scriptDecoder.TxoData
}

func (t *TxOut) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddUint64("v", t.Satoshi)
	return nil
}

type TxIns []*TxIn

func (tt TxIns) MarshalLogArray(arr zapcore.ArrayEncoder) error {
	var err error
	for i := range tt {
		err = multierr.Append(err, arr.AppendObject(tt[i]))
	}
	return err
}

type TxOuts []*TxOut

func (tt TxOuts) MarshalLogArray(arr zapcore.ArrayEncoder) error {
	var err error
	for i := range tt {
		err = multierr.Append(err, arr.AppendObject(tt[i]))
	}
	return err
}

// //////////////
type Block struct {
	Raw        []byte
	Hash       []byte // 32 bytes
	HashHex    string // 32 bytes
	FileIdx    int
	FileOffset int
	Height     int
	Txs        []*Tx
	Version    uint32
	MerkleRoot []byte // 32 bytes
	BlockTime  uint32
	Bits       uint32
	Nonce      uint32
	Size       uint32
	TxCnt      uint64
	Parent     []byte // 32 bytes
	ParentHex  string // 32 bytes
	NextHex    string // 32 bytes
	ParseData  *ProcessBlock
}

type BlockCache struct {
	Height     int
	Hash       []byte // 32 bytes
	TxCnt      uint64
	FileIdx    int
	FileOffset int
	Parent     []byte // 32 bytes
}

// //////////////
type ProcessBlock struct {
	Height           uint32
	AddrPkhInTxMap   map[string][]int
	SpentUtxoKeysMap map[string]struct{}
	SpentUtxoDataMap map[string]*TxoData
	NewUtxoDataMap   map[string]*TxoData
	TokenSummaryMap  map[string]*TokenData // key: CodeHash+GenesisId;  nft: CodeHash+GenesisId+tokenIndex
}

type TokenData struct {
	CodeType     uint32
	CodeHash     []byte
	GenesisId    []byte
	NFTIdx       uint64 // nft tokenIndex
	Decimal      uint8  // ft decimal
	InDataValue  uint64 // ft / nft count
	OutDataValue uint64 // ft / nft count
	InSatoshi    uint64
	OutSatoshi   uint64
}

type TxData struct {
	Raw  []byte
	TxId []byte // 32
}

type TxoData struct {
	UTxid       []byte
	Vout        uint32
	BlockHeight uint32
	TxIdx       uint64
	Satoshi     uint64
	PkScript    []byte
	ScriptType  []byte

	AddressData *scriptDecoder.TxoData
}

func (d *TxoData) Marshal(buf []byte) int {
	binary.LittleEndian.PutUint32(buf, d.BlockHeight) // 4
	// offset := scriptDecoder.PutVLQ(buf, uint64(d.BlockHeight))

	// 当前区块高度(<16777216)可以用3个字节编码，因此使用第4个字节标记启用压缩算法。
	// 以便解码时向前兼容非压缩算法
	buf[3] = 0x01 // is compress

	offset := 4
	offset += scriptDecoder.PutVLQ(buf[offset:], d.TxIdx)
	offset += scriptDecoder.PutVLQ(buf[offset:], scriptDecoder.CompressTxOutAmount(d.Satoshi))
	offset += scriptDecoder.PutCompressedScript(buf[offset:], d.PkScript)

	// binary.LittleEndian.PutUint32(buf, d.BlockHeight)  // 4
	// binary.LittleEndian.PutUint64(buf[4:], d.TxIdx)    // 8
	// binary.LittleEndian.PutUint64(buf[12:], d.Satoshi) // 8
	// copy(buf[20:], d.PkScript)                         // n

	return offset
}

// no need marshal: ScriptType, CodeType, CodeHash, GenesisId, AddressPkh, DataValue
func (d *TxoData) Unmarshal(buf []byte) {
	if buf[3] == 0x00 {
		// not compress
		d.BlockHeight = binary.LittleEndian.Uint32(buf[:4]) // 4
		d.TxIdx = binary.LittleEndian.Uint64(buf[4:12])     // 8
		d.Satoshi = binary.LittleEndian.Uint64(buf[12:20])  // 8
		d.PkScript = buf[20:]
		return
	}

	buf[3] = 0x00
	d.BlockHeight = binary.LittleEndian.Uint32(buf[:4]) // 4

	offset := 4
	txidx, bytesRead := scriptDecoder.DeserializeVLQ(buf[offset:])
	if bytesRead >= len(buf[offset:]) {
		// errors.New("unexpected end of data after txidx")
		return
	}
	d.TxIdx = txidx

	offset += bytesRead
	compressedAmount, bytesRead := scriptDecoder.DeserializeVLQ(buf[offset:])
	if bytesRead >= len(buf[offset:]) {
		// errors.New("unexpected end of data after compressed amount")
		return
	}

	offset += bytesRead
	// Decode the compressed script size and ensure there are enough bytes
	// left in the slice for it.
	scriptSize := scriptDecoder.DecodeCompressedScriptSize(buf[offset:])
	if len(buf[offset:]) < scriptSize {
		// errors.New("unexpected end of data after script size")
		return
	}

	d.Satoshi = scriptDecoder.DecompressTxOutAmount(compressedAmount)
	d.PkScript = scriptDecoder.DecompressScript(buf[offset : offset+scriptSize])

}

var TxoDataPool = sync.Pool{
	New: func() interface{} {
		return &TxoData{}
	},
}
