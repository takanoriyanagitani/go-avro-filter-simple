package enc

import (
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"
	ae "github.com/takanoriyanagitani/go-avro-filter-simple"
	. "github.com/takanoriyanagitani/go-avro-filter-simple/util"
)

func MapsToWriterHamba(
	ctx context.Context,
	imap iter.Seq2[map[string]any, error],
	wtr io.Writer,
	s ha.Schema,
	opts ...ho.EncoderFunc,
) error {
	enc, err := ho.NewEncoderWithSchema(
		s,
		wtr,
		opts...,
	)
	if nil != err {
		return err
	}
	defer enc.Close()

	for row, err := range imap {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if nil != err {
			return err
		}

		err = enc.Encode(row)
		if nil != err {
			return err
		}

		err = enc.Flush()
		if nil != err {
			return err
		}
	}

	return enc.Flush()
}

func CodecConv(c ae.Codec) ho.CodecName {
	switch c {
	case ae.CodecNull:
		return ho.Null
	case ae.CodecDeflate:
		return ho.Deflate
	case ae.CodecSnappy:
		return ho.Snappy
	case ae.CodecZstd:
		return ho.ZStandard

	// unsupported
	case ae.CodecBzip2:
		return ho.Null
	case ae.CodecXz:
		return ho.Null

	default:
		return ho.Null
	}
}

func ConfigToOpts(cfg ae.EncodeConfig) []ho.EncoderFunc {
	var c ho.CodecName = CodecConv(cfg.Codec)

	return []ho.EncoderFunc{
		ho.WithBlockLength(cfg.BlockLength),
		ho.WithCodec(c),
	}
}

func MapsToWriter(
	ctx context.Context,
	imap iter.Seq2[map[string]any, error],
	wtr io.Writer,
	schema string,
	cfg ae.EncodeConfig,
) error {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return e
	}

	var opts []ho.EncoderFunc = ConfigToOpts(cfg)

	return MapsToWriterHamba(
		ctx,
		imap,
		wtr,
		parsed,
		opts...,
	)
}

func MapsToStdout(
	ctx context.Context,
	imap iter.Seq2[map[string]any, error],
	schema string,
	cfg ae.EncodeConfig,
) error {
	return MapsToWriter(
		ctx,
		imap,
		os.Stdout,
		schema,
		cfg,
	)
}

func MapsToStdoutDefault(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	schema string,
) error {
	return MapsToStdout(ctx, m, schema, ae.EncodeConfigDefault)
}

func SchemaToMapsToStdoutDefault(
	schema string,
) func(iter.Seq2[map[string]any, error]) IO[Void] {
	return func(m iter.Seq2[map[string]any, error]) IO[Void] {
		return func(ctx context.Context) (Void, error) {
			return Empty, MapsToStdoutDefault(
				ctx,
				m,
				schema,
			)
		}
	}
}
