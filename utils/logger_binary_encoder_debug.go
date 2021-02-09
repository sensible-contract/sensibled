package utils

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func constructRowBinaryEncoderDebug(config zapcore.EncoderConfig) (zapcore.Encoder, error) {
	return &RowBinaryEncoderDebug{}, nil
}

////////////////
var _rowbinaryEncoderDebugPool = sync.Pool{
	New: func() interface{} {
		return &RowBinaryEncoderDebug{}
	},
}

func getRowBinaryEncoderDebug() *RowBinaryEncoderDebug {
	return _rowbinaryEncoderDebugPool.Get().(*RowBinaryEncoderDebug)
}

func putRowBinaryEncoderDebug(e *RowBinaryEncoderDebug) {
	_rowbinaryEncoderDebugPool.Put(e)
}

////////////////
type RowBinaryEncoderDebug struct {
	*zapcore.MapObjectEncoder
}

func (enc *RowBinaryEncoderDebug) Clone() zapcore.Encoder {
	return getRowBinaryEncoderDebug()
}

func (enc *RowBinaryEncoderDebug) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	myEnc := enc.Clone().(*RowBinaryEncoderDebug)
	buf := _pool.Get()

	for _, f := range fields {
		switch f.Type {
		case zapcore.BinaryType:
			buf.AppendByte(' ')

			src := f.Interface.([]byte)
			dst := make([]byte, hex.EncodedLen(len(src)))
			hex.Encode(dst, src)
			buf.Write(dst)
		case zapcore.BoolType:
			buf.AppendByte(' ')

			if f.Integer == 1 {
				buf.AppendByte('0')
				buf.AppendByte('1')
			} else {
				buf.AppendByte('0')
				buf.AppendByte('0')
			}

		case zapcore.ByteStringType:
			buf.AppendByte(' ')

			var b []byte
			b = appendUleb128(b, uint64(len(f.Interface.([]byte))))

			dstb := make([]byte, hex.EncodedLen(len(b)))
			hex.Encode(dstb, b)
			buf.Write(dstb)

			buf.AppendByte(' ')
			src := f.Interface.([]byte)
			dst := make([]byte, hex.EncodedLen(len(src)))
			hex.Encode(dst, src)
			buf.Write(dst)

		case zapcore.Uint64Type:
			buf.AppendByte(' ')
			src := make([]byte, 8)
			binary.LittleEndian.PutUint64(src, uint64(f.Integer))
			dst := make([]byte, 16)
			hex.Encode(dst, src)
			buf.Write(dst)

		case zapcore.Uint32Type:
			buf.AppendByte(' ')
			src := make([]byte, 4)
			binary.LittleEndian.PutUint32(src, uint32(f.Integer))
			dst := make([]byte, 8)
			hex.Encode(dst, src)
			buf.Write(dst)

		case zapcore.Uint16Type:
			buf.AppendByte(' ')
			src := make([]byte, 2)
			binary.LittleEndian.PutUint16(src, uint16(f.Integer))
			dst := make([]byte, 4)
			hex.Encode(dst, src)
			buf.Write(dst)

		case zapcore.Uint8Type:
			buf.AppendByte(' ')
			src := []byte{uint8(f.Integer)}
			dst := make([]byte, 2)
			hex.Encode(dst, src)
			buf.Write(dst)

		default:
			panic(fmt.Sprintf("unknown field type: %v", f))
		}
	}
	buf.AppendByte('\n')
	putRowBinaryEncoderDebug(myEnc)
	return buf, nil
}
