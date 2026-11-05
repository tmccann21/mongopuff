package transform

import (
	"errors"
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tmccann21/mongopuff/internal/config"
)

// resolveNamespace resolves ns against doc. A {{field}} that is absent from
// doc or null fails, as does a value formatNamespaceValue rejects.
func resolveNamespace(ns config.Namespace, doc map[string]any) (string, error) {
	return ns.Resolve(func(field string) (string, error) {
		val, ok := doc[field]
		if !ok || val == nil {
			return "", errors.New("absent or null")
		}
		return formatNamespaceValue(val)
	})
}

// formatNamespaceValue formats a {{field}} value as namespace text. It accepts
// only types with one obvious printed form: a string as-is, an integer or
// whole-number double in decimal, an ObjectID as hex, and a UUID hyphenated.
func formatNamespaceValue(val any) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case int32, int64, float64:
		n, err := coerceInt(v)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(n.(int64), 10), nil
	case bson.ObjectID:
		return v.Hex(), nil
	case bson.Binary:
		s, err := coerceUUID(v)
		if err != nil {
			return "", err
		}
		return s.(string), nil
	default:
		return "", fmt.Errorf("unsupported type %T", val)
	}
}
