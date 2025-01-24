package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	ae "github.com/takanoriyanagitani/go-avro-filter-simple"
	sh "github.com/takanoriyanagitani/go-avro-filter-simple/avro/avsc/hamba"
	dh "github.com/takanoriyanagitani/go-avro-filter-simple/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-filter-simple/avro/enc/hamba"
	. "github.com/takanoriyanagitani/go-avro-filter-simple/util"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.StdinToMapsDefault

var targetColumnName IO[string] = EnvValByKey("ENV_TARGET_COL_NAME")

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		file, err := os.Open(filename)
		if nil != err {
			return "", fmt.Errorf("%w: filename=%s", err, filename)
		}

		limited := &io.LimitedReader{
			R: file,
			N: limit,
		}

		var buf strings.Builder
		_, err = io.Copy(&buf, limited)

		return buf.String(), err
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var targetType IO[ae.PrimitiveType] = Bind(
	All(
		schemaContent,
		targetColumnName,
	),
	Lift(func(s []string) (ae.PrimitiveType, error) {
		return sh.SchemaToType(s[0], s[1])
	}),
)

var targetValueString IO[string] = EnvValByKey("ENV_TARGET_VALUE")

var rawTargetConfig IO[ae.RawTargetConfig] = Bind(
	All(
		targetColumnName,
		targetValueString,
	),
	Lift(func(s []string) (ae.RawTargetConfig, error) {
		return ae.RawTargetConfig{
			TargetColumnName: ae.TargetColumnName(s[0]),
			TargetValue:      s[1],
		}, nil
	}),
)

var filter IO[ae.Filter] = Bind(
	targetType,
	func(typ ae.PrimitiveType) IO[ae.Filter] {
		return Bind(
			rawTargetConfig,
			Lift(func(c ae.RawTargetConfig) (ae.Filter, error) {
				return c.ToFilter(typ)
			}),
		)
	},
)

var filtered IO[iter.Seq2[map[string]any, error]] = Bind(
	filter,
	func(f ae.Filter) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			stdin2maps,
			func(
				all iter.Seq2[map[string]any, error],
			) IO[iter.Seq2[map[string]any, error]] {
				return func(
					ctx context.Context,
				) (iter.Seq2[map[string]any, error], error) {
					return f.MapsToMaps(ctx, all), nil
				}
			},
		)
	},
)

var stdin2avro2maps2filtered2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(s string) IO[Void] {
		return Bind(
			filtered,
			eh.SchemaToMapsToStdoutDefault(s),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2avro2maps2filtered2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
