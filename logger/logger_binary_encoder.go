package logger

import (
	"encoding/binary"
	"fmt"
	"sync"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var (
	_pool = buffer.NewPool()
	Get   = _pool.Get
)

func constructRowBinaryEncoder(config zapcore.EncoderConfig) (zapcore.Encoder, error) {
	return &RowBinaryEncoder{}, nil
}

////////////////
var _rowbinaryEncoderPool = sync.Pool{
	New: func() interface{} {
		return &RowBinaryEncoder{}
	},
}

func getRowBinaryEncoder() *RowBinaryEncoder {
	return _rowbinaryEncoderPool.Get().(*RowBinaryEncoder)
}

func putRowBinaryEncoder(e *RowBinaryEncoder) {
	_rowbinaryEncoderPool.Put(e)
}

////////////////
type RowBinaryEncoder struct {
	*zapcore.MapObjectEncoder
}

func (enc *RowBinaryEncoder) Clone() zapcore.Encoder {
	return getRowBinaryEncoder()
}

func (enc *RowBinaryEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	myEnc := enc.Clone().(*RowBinaryEncoder)
	buf := _pool.Get()

	for _, f := range fields {
		switch f.Type {
		case zapcore.BinaryType:
			buf.Write(f.Interface.([]byte))
		case zapcore.BoolType:
			if f.Integer == 1 {
				binary.Write(buf, binary.LittleEndian, int8(1))
			} else {
				binary.Write(buf, binary.LittleEndian, int8(-1))
			}
		case zapcore.ByteStringType:
			var b []byte
			b = appendUleb128(b, uint64(len(f.Interface.([]byte))))
			buf.Write(b)
			buf.Write(f.Interface.([]byte))
		case zapcore.Uint64Type:
			binary.Write(buf, binary.LittleEndian, uint64(f.Integer))
		case zapcore.Uint32Type:
			binary.Write(buf, binary.LittleEndian, uint32(f.Integer))
		case zapcore.Uint16Type:
			binary.Write(buf, binary.LittleEndian, uint16(f.Integer))
		case zapcore.Uint8Type:
			binary.Write(buf, binary.LittleEndian, uint8(f.Integer))
		default:
			panic(fmt.Sprintf("unknown field type: %v", f))
		}
	}
	// buf.AppendByte('\n')
	putRowBinaryEncoder(myEnc)
	return buf, nil
}

// https://github.com/golang/go/blob/0ff9df6b53076a9402f691b07707f7d88d352722/src/cmd/internal/dwarf/dwarf.go#L194
// AppendUleb128 appends v to b using DWARF's unsigned LEB128 encoding.
func appendUleb128(b []byte, v uint64) []byte {
	for {
		c := uint8(v & 0x7f)
		v >>= 7
		if v != 0 {
			c |= 0x80
		}
		b = append(b, c)
		if c&0x80 == 0 {
			break
		}
	}
	return b
}
