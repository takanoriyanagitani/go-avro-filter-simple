package filt

import (
	"context"
	"database/sql"
	"errors"
	"iter"
	"maps"
	"strconv"
)

var (
	ErrInvalidInput error = errors.New("invalid input")
	ErrInvalidType  error = errors.New("invalid type")
)

type TargetColumnName string

type TargetConfig[T comparable] struct {
	TargetColumnName
	TargetValue T
}

type RawTargetConfig TargetConfig[string]

func (r RawTargetConfig) ToFilter(t PrimitiveType) (Filter, error) {
	s2f, e := t.ToStringToFilter(r.TargetColumnName)
	if nil != e {
		return nil, e
	}
	return s2f(r.TargetValue)
}

type StringToTyped[T comparable] func(string) (T, error)

var StringToBoolean StringToTyped[bool] = strconv.ParseBool

func StrToStr(s string) (string, error) { return s, nil }

var StringToString StringToTyped[string] = StrToStr

var StringToInteger StringToTyped[int] = strconv.Atoi

func StrToInt(s string) (int32, error) {
	i, e := strconv.ParseInt(s, 10, 32)
	return int32(i), e
}

func StrToLong(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

var StringToInt StringToTyped[int32] = StrToInt

var StringToLong StringToTyped[int64] = StrToLong

type PrimitiveType string

const (
	PrimitiveUnspecified PrimitiveType = "UNSPECIFIED"
	PrimitiveString      PrimitiveType = "string"
	PrimitiveInt         PrimitiveType = "int"
	PrimitiveLong        PrimitiveType = "long"
	PrimitiveFloat       PrimitiveType = "float"
	PrimitiveDouble      PrimitiveType = "double"
	PrimitiveBool        PrimitiveType = "boolean"
)

func StrToFloat(s string) (float32, error) {
	f, e := strconv.ParseFloat(s, 32)
	return float32(f), e
}

func StrToDouble(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

var StringToFloat StringToTyped[float32] = StrToFloat

var StringToDouble StringToTyped[float64] = StrToDouble

type StringToConfig[T comparable] func(string) (TargetConfig[T], error)

type StringToFilter func(string) (Filter, error)

func (c StringToConfig[T]) ToStringToFilter() StringToFilter {
	return func(s string) (Filter, error) {
		tcfg, e := c(s)
		if nil != e {
			return nil, e
		}
		return tcfg.ToFilter(), nil
	}
}

func (c StringToTyped[T]) ToStrToConfig(n TargetColumnName) StringToConfig[T] {
	return func(s string) (TargetConfig[T], error) {
		t, e := c(s)
		return TargetConfig[T]{
			TargetColumnName: n,
			TargetValue:      t,
		}, e
	}
}

func (c StringToTyped[T]) ToStringToFilter(n TargetColumnName) StringToFilter {
	var s2c StringToConfig[T] = c.ToStrToConfig(n)
	return s2c.ToStringToFilter()
}

func (p PrimitiveType) AsString() string {
	var s string = string(p)
	return s
}

var PrimitiveTypeToStringMap map[PrimitiveType]string = maps.Collect(
	func(yield func(PrimitiveType, string) bool) {
		yield(PrimitiveUnspecified, PrimitiveUnspecified.AsString())
		yield(PrimitiveString, PrimitiveString.AsString())
		yield(PrimitiveInt, PrimitiveInt.AsString())
		yield(PrimitiveLong, PrimitiveLong.AsString())
		yield(PrimitiveFloat, PrimitiveFloat.AsString())
		yield(PrimitiveDouble, PrimitiveDouble.AsString())
		yield(PrimitiveBool, PrimitiveBool.AsString())
	},
)

var StringToPrimitiveTypeMap map[string]PrimitiveType = maps.Collect(
	func(yield func(string, PrimitiveType) bool) {
		for key, val := range PrimitiveTypeToStringMap {
			yield(val, key)
		}
	},
)

func PrimitiveTypeFromString(s string) (PrimitiveType, error) {
	typ, found := StringToPrimitiveTypeMap[s]
	switch found {
	case true:
		return typ, nil
	default:
		return PrimitiveUnspecified, ErrInvalidType
	}
}

func (p PrimitiveType) ToStringToFilter(
	n TargetColumnName,
) (StringToFilter, error) {
	switch p {

	case PrimitiveUnspecified:
		return nil, ErrInvalidType

	case PrimitiveString:
		return StringToString.ToStringToFilter(n), nil

	case PrimitiveInt:
		return StringToInt.ToStringToFilter(n), nil

	case PrimitiveLong:
		return StringToLong.ToStringToFilter(n), nil

	case PrimitiveFloat:
		return StringToFloat.ToStringToFilter(n), nil

	case PrimitiveDouble:
		return StringToDouble.ToStringToFilter(n), nil

	case PrimitiveBool:
		return StringToBoolean.ToStringToFilter(n), nil

	default:
		return nil, ErrInvalidType

	}
}

type Row map[string]any

type FilterResult int

const (
	FilterResultUnspecified FilterResult = 0
	FilterResultMatch       FilterResult = iota
	FilterResultUnmatch     FilterResult = iota
)

type Filter func(Row) (FilterResult, error)

func (c TargetConfig[T]) ConvertAny(a any) (sql.Null[T], error) {
	var ret sql.Null[T]

	switch typ := a.(type) {
	case T:
		return sql.Null[T]{V: typ, Valid: true}, nil
	case nil:
		return ret, nil
	default:
		return ret, ErrInvalidInput
	}
}

func (c TargetConfig[T]) Compare(target T) FilterResult {
	switch target == c.TargetValue {
	case true:
		return FilterResultMatch
	default:
		return FilterResultUnmatch
	}
}

func (c TargetConfig[T]) ToFilter() Filter {
	return func(r Row) (FilterResult, error) {
		var a any = r[string(c.TargetColumnName)]
		n, e := c.ConvertAny(a)
		if nil != e {
			return FilterResultUnspecified, e
		}

		switch n.Valid {
		case true:
			return c.Compare(n.V), nil
		default:
			return FilterResultUnmatch, nil
		}
	}
}

func (f Filter) MapsToMaps(
	ctx context.Context,
	all iter.Seq2[map[string]any, error],
) (filtered iter.Seq2[map[string]any, error]) {
	return func(yield func(map[string]any, error) bool) {
		for row, e := range all {
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			default:
			}

			if nil != e {
				yield(nil, e)
				return
			}

			res, e := f(row)
			if nil != e {
				yield(nil, e)
				return
			}

			if FilterResultMatch == res {
				if !yield(row, nil) {
					return
				}
			}
		}
	}
}

const BlobSizeMaxDefault int = 1048576

type DecodeConfig struct{ BlobSizeMax int }

var DecodeConfigDefault DecodeConfig = DecodeConfig{
	BlobSizeMax: BlobSizeMaxDefault,
}

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

const BlockLengthDefault int = 100

type EncodeConfig struct {
	BlockLength int
	Codec
}

var EncodeConfigDefault EncodeConfig = EncodeConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}
