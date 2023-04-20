package model

import (
	"encoding/binary"
	"sync"
	scriptDecoder "unisatd/parser/script"

	"go.uber.org/zap/zapcore"
)

const MEMPOOL_HEIGHT = 0x3fffff // 4294967295 2^32-1; 3fffff, 2^22-1
const HEIGHT_MUTIPLY = 1000000000

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

	NewNFTDataCreated []*scriptDecoder.NFTData
	NFTInputsCnt      uint64
	NFTOutputsCnt     uint64
	NFTLostCnt        uint64

	OpInRBF       bool
	GenesisNewNFT bool
}

type TxIn struct {
	InputHashHex string // 32
	InputHash    []byte // 32
	InputVout    uint32
	ScriptSig    []byte
	Sequence     uint32

	ScriptWitness []byte

	// other:
	CreatePointOfNFTs         []*NFTCreatePoint // 输入的nft
	CreatePointCountOfNewNFTs uint32            // 新创建的nft, 有些如果是重复创建,则标记invalid

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

// //////////////
type ProcessBlock struct {
	Height               uint32
	AddrPkhInTxMap       map[string][]int
	SpentUtxoKeysMap     map[string]struct{}
	SpentUtxoDataMap     map[string]*TxoData
	NewUtxoDataMap       map[string]*TxoData
	NewInscriptions      []*NewInscriptionInfo // index: createBlockNFTIndex;  nft: IncriptionID
	NewBRC20Inscriptions []*NewInscriptionInfo
}

type NewInscriptionInfo struct {
	NFTData      *scriptDecoder.NFTData // type/data
	CreatePoint  *NFTCreatePoint
	Height       uint32 // for brc20 to height
	TxIdx        uint64 // txidx in block
	TxId         []byte // create txid
	IdxInTx      uint32 // nft idx inside tx
	InTxVout     uint32 // nft outgoing(vout) inside tx
	InputsValue  uint64
	OutputsValue uint64
	Satoshi      uint64
	PkScript     []byte
	Ordinal      uint64
	Number       uint64
	BlockTime    uint32
}

func (d *NewInscriptionInfo) DumpString() string {
	var data [80]byte
	binary.LittleEndian.PutUint32(data[0:4], d.CreatePoint.Height) // fixme: may nil
	binary.LittleEndian.PutUint32(data[4:8], d.BlockTime)
	binary.LittleEndian.PutUint64(data[8:16], d.InputsValue)
	binary.LittleEndian.PutUint64(data[16:24], d.OutputsValue)
	binary.LittleEndian.PutUint64(data[24:32], d.Ordinal)
	binary.LittleEndian.PutUint64(data[32:40], d.Number)

	copy(data[40:72], d.TxId[:])

	binary.LittleEndian.PutUint32(data[72:76], d.IdxInTx)
	binary.LittleEndian.PutUint32(data[76:80], uint32(len(d.NFTData.ContentBody)))

	return string(data[:]) + string(d.NFTData.ContentType)
}

type TxData struct {
	Raw  []byte
	TxId []byte // 32
}

// nft create point on create
type NFTCreatePoint struct {
	Height     uint32 // Height of NFT show in block onCreate
	IdxInBlock uint64 // Index of NFT show in block onCreate
	Offset     uint64 // sat offset in utxo
	HasMoved   bool   // the NFT has been moved after created
	IsBRC20    bool   // the NFT is BRC20
}

func (p *NFTCreatePoint) GetCreateIdxKey() string {
	var key [12]byte
	binary.LittleEndian.PutUint32(key[0:4], p.Height)
	binary.LittleEndian.PutUint64(key[4:12], p.IdxInBlock)
	return string(key[:])
}

type TxoData struct {
	UTxid       []byte
	Vout        uint32
	BlockHeight uint32
	TxIdx       uint64
	Satoshi     uint64
	PkScript    []byte
	ScriptType  []byte
	OpInRBF     bool

	CreatePointOfNFTs []*NFTCreatePoint

	AddressData *scriptDecoder.AddressData
}

func (d *TxoData) Marshal(buf []byte) int {
	binary.LittleEndian.PutUint32(buf, d.BlockHeight) // 4
	// offset := scriptDecoder.PutVLQ(buf, uint64(d.BlockHeight))

	// 当前区块高度(<16777216)可以用3个字节编码，因此使用第4个字节标记启用压缩算法。
	// 以便解码时向前兼容非压缩算法
	// buf[3] = 0x01 // is compress

	offset := 4
	offset += scriptDecoder.PutVLQ(buf[offset:], d.TxIdx)
	offset += scriptDecoder.PutVLQ(buf[offset:], scriptDecoder.CompressTxOutAmount(d.Satoshi))
	offset += scriptDecoder.PutCompressedScript(buf[offset:], d.PkScript)

	if d.BlockHeight == MEMPOOL_HEIGHT {
		if d.OpInRBF {
			buf[offset] = 0x01
		} else {
			buf[offset] = 0x00
		}
		offset += 1
	}

	offset += DumpNFTCreatePoints(buf[offset:], d.CreatePointOfNFTs)

	// binary.LittleEndian.PutUint32(buf, d.BlockHeight)  // 4
	// binary.LittleEndian.PutUint64(buf[4:], d.TxIdx)    // 8
	// binary.LittleEndian.PutUint64(buf[12:], d.Satoshi) // 8
	// copy(buf[20:], d.PkScript)                         // n

	return offset
}

// no need marshal: ScriptType, CodeType, CodeHash, GenesisId, AddressPkh, DataValue
func (d *TxoData) Unmarshal(buf []byte) {
	// if buf[3] == 0x01 {
	// 	buf[3] = 0x00
	// not compress
	// d.BlockHeight = binary.LittleEndian.Uint32(buf[:4]) // 4
	// d.TxIdx = binary.LittleEndian.Uint64(buf[4:12])     // 8
	// d.Satoshi = binary.LittleEndian.Uint64(buf[12:20])  // 8
	// d.PkScript = buf[20:]
	// return
	// }

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

	if d.BlockHeight == MEMPOOL_HEIGHT {
		if buf[offset] == 0x01 {
			d.OpInRBF = true
		}
		offset += 1
	}

	offset += d.LoadNFTCreatePointsFromRaw(buf[offset:])
}

// dump nft
func DumpNFTCreatePoints(buf []byte, createPointOfNFTs []*NFTCreatePoint) int {
	offset := 0
	for _, nft := range createPointOfNFTs {
		offset += scriptDecoder.PutVLQ(buf[offset:], uint64(nft.Height))
		offset += scriptDecoder.PutVLQ(buf[offset:], nft.IdxInBlock)
		offset += scriptDecoder.PutVLQ(buf[offset:], nft.Offset)
		if nft.HasMoved {
			buf[offset] = 0x01
		} else {
			buf[offset] = 0
		}
		if nft.IsBRC20 {
			buf[offset] += 0x02
		}
		offset += 1
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

		hasMoved := false
		if buf[offset]&0x01 == 0x01 {
			hasMoved = true
		}
		isBRC20 := false
		if buf[offset]&0x02 == 0x02 {
			isBRC20 = true
		}
		offset += 1

		d.CreatePointOfNFTs = append(d.CreatePointOfNFTs, &NFTCreatePoint{
			Height:     uint32(height),
			IdxInBlock: nftIdx,
			Offset:     satOffset,
			HasMoved:   hasMoved,
			IsBRC20:    isBRC20,
		})
	}
}

var TxoDataPool = sync.Pool{
	New: func() interface{} {
		return &TxoData{}
	},
}
