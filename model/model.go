package model

import (
	"encoding/binary"
	scriptDecoder "sensibled/parser/script"
	"sync"

	"go.uber.org/zap/zapcore"
)

const MEMPOOL_HEIGHT = 4294967295

type Tx struct {
	Raw          []byte
	TxIdHex      string // 64
	TxId         []byte // 32
	Size         uint32
	WitOffset    uint32
	LockTime     uint32
	Version      uint32
	TxInCnt      uint32
	TxOutCnt     uint32
	InputsValue  uint64
	OutputsValue uint64
	TxIns        []*TxIn
	TxOuts       []*TxOut

	CreateNFTData []*scriptDecoder.NFTData
	NFTInputsCnt  uint64
	NFTOutputsCnt uint64
	NFTLostCnt    uint64

	IsSensible bool
}

type TxIn struct {
	InputHashHex string // 32
	InputHash    []byte // 32
	InputVout    uint32
	ScriptSig    []byte
	Sequence     uint32

	ScriptWitness []byte

	// other:
	CreatePointOfNewNFTs []*NFTCreatePoint // 新创建的nft, 有些如果是重复创建,则标记invalid
	CreatePointOfNFTs    []*NFTCreatePoint // 输入的nft

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

	Outpoint      []byte // 32 + 4
	OutpointKey   string // 32 + 4
	ScriptType    []byte
	ScriptTypeHex string

	AddressData *scriptDecoder.AddressData

	CreatePointOfNFTs []*NFTCreatePoint

	LockingScriptUnspendable bool
}

type TxWit struct {
	Script []byte
}

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

////////////////

type TxLocation struct {
	BlockHeight uint32
	TxIdx       uint64
}

type ProcessBlock struct {
	Height                 uint32
	AddrPkhInTxMap         map[string][]int
	SpentUtxoKeysMap       map[string]struct{}
	SpentUtxoDataMap       map[string]*TxoData
	NewUtxoDataMap         map[string]*TxoData
	NFTsCreateIndexToNFTID []*InscriptionID // index: createBlockNFTIndex;  nft: IncriptionID
}

type InscriptionID struct {
	TxId   []byte // 32
	NFTIdx uint64 // nft idx inside tx
}

type TxData struct {
	Raw  []byte
	TxId []byte // 32
}

// nft create point on create
type NFTCreatePoint struct {
	Height uint32 // Height of NFT show in block onCreate
	Idx    uint64 // Index of NFT show in block onCreate
	Offset uint64 // sat offset in utxo
}

type TxoData struct {
	UTxid       []byte
	Vout        uint32
	BlockHeight uint32
	TxIdx       uint64
	Satoshi     uint64
	PkScript    []byte
	ScriptType  []byte

	CreatePointOfNFTs []*NFTCreatePoint

	AddressData *scriptDecoder.AddressData
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
	offset += DumpNFTCreatePoints(buf[offset:], d.CreatePointOfNFTs)

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
		return
	}
	d.TxIdx = txidx
	offset += bytesRead

	compressedAmount, bytesRead := scriptDecoder.DeserializeVLQ(buf[offset:])
	if bytesRead >= len(buf[offset:]) {
		return
	}
	offset += bytesRead

	// Decode the compressed script size and ensure there are enough bytes
	// left in the slice for it.
	scriptSize := scriptDecoder.DecodeCompressedScriptSize(buf[offset:])
	if len(buf[offset:]) < scriptSize {
		return
	}

	d.Satoshi = scriptDecoder.DecompressTxOutAmount(compressedAmount)
	d.PkScript = scriptDecoder.DecompressScript(buf[offset : offset+scriptSize])
	offset += scriptSize
	offset += d.LoadNFTCreatePointsFromRaw(buf[offset:])
}

// dump nft
func DumpNFTCreatePoints(buf []byte, createPointOfNFTs []*NFTCreatePoint) int {
	offset := 0
	for _, nft := range createPointOfNFTs {
		offset += scriptDecoder.PutVLQ(buf[offset:], uint64(nft.Height))
		offset += scriptDecoder.PutVLQ(buf[offset:], nft.Idx)
		offset += scriptDecoder.PutVLQ(buf[offset:], nft.Offset)
	}
	return offset
}

// load nft
func (d *TxoData) LoadNFTCreatePointsFromRaw(buf []byte) (offset int) {
	for {
		if len(buf[offset:]) == 0 {
			return
		}

		height, bytesRead := scriptDecoder.DeserializeVLQ(buf[offset:])
		if bytesRead >= len(buf[offset:]) {
			return
		}
		offset += bytesRead

		nftIdx, bytesRead := scriptDecoder.DeserializeVLQ(buf[offset:])
		if bytesRead >= len(buf[offset:]) {
			return
		}
		offset += bytesRead

		satOffset, bytesRead := scriptDecoder.DeserializeVLQ(buf[offset:])
		if bytesRead > len(buf[offset:]) {
			return
		}
		offset += bytesRead

		d.CreatePointOfNFTs = append(d.CreatePointOfNFTs, &NFTCreatePoint{
			Height: uint32(height),
			Idx:    nftIdx,
			Offset: satOffset,
		})
	}
}

var TxoDataPool = sync.Pool{
	New: func() interface{} {
		return &TxoData{}
	},
}
