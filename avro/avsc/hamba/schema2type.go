package avsc

import (
	"errors"

	ha "github.com/hamba/avro/v2"
	ae "github.com/takanoriyanagitani/go-avro-filter-simple"
)

var (
	ErrInvalidType   error = errors.New("invalid primitive schema")
	ErrInvalidField  error = errors.New("invalid field")
	ErrInvalidSchema error = errors.New("invalid schema")
	ErrInvalidUnion  error = errors.New("invalid union")
)

func PrimitiveSchemaToType(s *ha.PrimitiveSchema) (ae.PrimitiveType, error) {
	var typ ha.Type = s.Type()

	switch typ {

	case ha.String:
		return ae.PrimitiveString, nil
	case ha.Int:
		return ae.PrimitiveInt, nil
	case ha.Long:
		return ae.PrimitiveLong, nil
	case ha.Float:
		return ae.PrimitiveFloat, nil
	case ha.Double:
		return ae.PrimitiveDouble, nil
	case ha.Boolean:
		return ae.PrimitiveBool, nil

	default:
		return ae.PrimitiveUnspecified, ErrInvalidType

	}
}

func UnionSchemaToType(u *ha.UnionSchema) (ae.PrimitiveType, error) {
	var s []ha.Schema = u.Types()
	for _, typ := range s {
		switch t := typ.(type) {
		case *ha.PrimitiveSchema:
			return PrimitiveSchemaToType(t)
		default:
			continue
		}
	}
	return ae.PrimitiveUnspecified, ErrInvalidUnion
}

func FieldToType(f *ha.Field) (ae.PrimitiveType, error) {
	var s ha.Schema = f.Type()
	switch typ := s.(type) {
	case *ha.PrimitiveSchema:
		return PrimitiveSchemaToType(typ)
	case *ha.UnionSchema:
		return UnionSchemaToType(typ)
	default:
		return ae.PrimitiveUnspecified, ErrInvalidField
	}
}

func RecordSchemaToType(
	r *ha.RecordSchema,
	colname string,
) (ae.PrimitiveType, error) {
	var fields []*ha.Field = r.Fields()
	for _, field := range fields {
		var name string = field.Name()
		if name == colname {
			return FieldToType(field)
		}
	}
	return ae.PrimitiveUnspecified, ErrInvalidSchema
}

func SchemaToTypeHamba(
	s ha.Schema,
	colname string,
) (ae.PrimitiveType, error) {
	switch typ := s.(type) {
	case *ha.RecordSchema:
		return RecordSchemaToType(typ, colname)
	default:
		return ae.PrimitiveUnspecified, ErrInvalidSchema
	}
}

func SchemaToType(
	schema string,
	colname string,
) (ae.PrimitiveType, error) {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return ae.PrimitiveUnspecified, e
	}
	return SchemaToTypeHamba(parsed, colname)
}
