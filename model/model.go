package model

import (
	"encoding/binary"
	"sync"

	"go.uber.org/multierr"
	"go.uber.org/zap/zapcore"
)

const MEMPOOL_HEIGHT = 4294967295

type Tx struct {
	Raw          []byte
	HashHex      string // 32
	Hash         []byte // 32
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

	// other:
	CodeType   uint32
	CodeHash   []byte
	GenesisId  []byte
	SensibleId []byte
	AddressPkh []byte

	// nft
	MetaTxId        []byte
	MetaOutputIndex uint32
	TokenIndex      uint64
	TokenSupply     uint64

	// ft
	Amount  uint64
	Decimal uint8
	Name    string
	Symbol  string

	Outpoint                 []byte // 32 + 4
	OutpointKey              string // 32 + 4
	ScriptType               []byte
	ScriptTypeHex            string
	LockingScriptUnspendable bool
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

////////////////
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
type ProcessBlock struct {
	Height           uint32
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

type TxoData struct {
	UTxid       []byte
	Vout        uint32
	BlockHeight uint32
	TxIdx       uint64

	CodeType   uint32
	CodeHash   []byte
	GenesisId  []byte // for search: codehash + genesis
	SensibleId []byte // GenesisTx outpoint
	AddressPkh []byte

	// nft
	MetaTxId        []byte
	MetaOutputIndex uint32
	TokenIndex      uint64
	TokenSupply     uint64

	// ft
	Name    string
	Symbol  string
	Amount  uint64
	Decimal uint8

	Satoshi    uint64
	PkScript   []byte
	ScriptType []byte
}

func (d *TxoData) Marshal(buf []byte) {
	binary.LittleEndian.PutUint32(buf, d.BlockHeight)  // 4
	binary.LittleEndian.PutUint64(buf[4:], d.TxIdx)    // 8
	binary.LittleEndian.PutUint64(buf[12:], d.Satoshi) // 8
	copy(buf[20:], d.PkScript)                         // n
}

// no need marshal: ScriptType, CodeType, CodeHash, GenesisId, AddressPkh, DataValue
func (d *TxoData) Unmarshal(buf []byte) {
	d.BlockHeight = binary.LittleEndian.Uint32(buf[:4]) // 4
	d.TxIdx = binary.LittleEndian.Uint64(buf[4:12])     // 8
	d.Satoshi = binary.LittleEndian.Uint64(buf[12:20])  // 8
	d.PkScript = buf[20:]
	// d.Script = make([]byte, len(buf)-20)
	// copy(d.Script, buf[20:]) // n
}

var TxoDataPool = sync.Pool{
	New: func() interface{} {
		return &TxoData{}
	},
}
