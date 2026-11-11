package transform

import (
	"errors"
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tmccann21/mongopuff/internal/config"
)

// errImageUnavailable is the failure for a namespace resolved from a document
// image the event does not carry: an update or replace without its post-image,
// or an update, replace or delete without its pre-image.
var errImageUnavailable = errors.New("document image unavailable")

// resolveNamespace resolves ns against doc. A nil doc fails, as does a
// {{field}} that is absent from doc or null, a value formatNamespaceValue
// rejects, or a resolved name that breaks turbopuffer's rules. Every failure
// wraps ErrNamespaceUnresolved. A static namespace never reads doc, so the
// static stream, which requests no images, resolves with a nil one.
func resolveNamespace(ns config.Namespace, doc map[string]any) (string, error) {
	name, err := ns.Resolve(func(field string) (string, error) {
		if doc == nil {
			return "", errImageUnavailable
		}
		val, ok := doc[field]
		if !ok || val == nil {
			return "", errors.New("absent or null")
		}
		return formatNamespaceValue(val)
	})
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrNamespaceUnresolved, err)
	}
	return name, nil
}

// formatNamespaceValue formats a {{field}} value as namespace text. It accepts
// only types with one obvious printed form: a string as-is, an integer or
// whole-number double in decimal, an ObjectID as hex, and a UUID hyphenated.
func formatNamespaceValue(val any) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case int32, int64, float64:
		n, err := toInt64(v)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(n, 10), nil
	case bson.ObjectID:
		return v.Hex(), nil
	case bson.Binary:
		return uuidString(v)
	default:
		return "", fmt.Errorf("unsupported type %T", val)
	}
}
