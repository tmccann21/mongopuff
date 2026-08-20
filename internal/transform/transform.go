package transform

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/mongo"
)

type ActionType int

const (
	ActionUpsert ActionType = iota
	ActionPatch
	ActionDelete
	ActionSkip
)

// Action is what gets sent to turbopuffer after transforming a change event.
type Action struct {
	Type        ActionType
	DocumentID  string
	Attributes  map[string]any // field name → value
	ClusterTime uint64
}

// SerializeID converts a BSON _id value to a string for use as a turbopuffer document ID.
// Supported types: ObjectID, string, int32, int64, Binary with UUID subtype.
// All other types are rejected.
func SerializeID(id any) (string, error) {
	if id == nil {
		return "", fmt.Errorf("_id is nil")
	}

	switch v := id.(type) {
	case bson.ObjectID:
		return v.Hex(), nil
	case string:
		return v, nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case bson.Binary:
		if v.Subtype != bson.TypeBinaryUUID {
			return "", fmt.Errorf("unsupported _id binary subtype: 0x%02x", v.Subtype)
		}
		if len(v.Data) != 16 {
			return "", fmt.Errorf("UUID binary _id has invalid length %d, expected 16", len(v.Data))
		}
		return formatUUID(v.Data), nil
	default:
		return "", fmt.Errorf("unsupported _id type: %T", id)
	}
}

func formatUUID(b []byte) string {
	return hex.EncodeToString(b[0:4]) + "-" +
		hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" +
		hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
}

// SerializeClusterTime converts a BSON Timestamp to a uint64 preserving total ordering.
// The encoding is: uint64(T)<<32 | uint64(I).
func SerializeClusterTime(t uint32, i uint32) uint64 {
	return uint64(t)<<32 | uint64(i)
}

// MapChangeEvent converts a change stream event into a turbopuffer Action
// according to the collection's field mapping config.
func MapChangeEvent(event mongo.ChangeEvent, coll config.CollectionConfig) (Action, error) {
	switch event.Operation {
	case mongo.OpInsert, mongo.OpReplace:
		return mapUpsert(event, coll)
	case mongo.OpUpdate:
		return mapPatch(event, coll)
	case mongo.OpDelete:
		return mapDelete(event, coll)
	case mongo.OpInvalidate:
		return Action{}, fmt.Errorf("change stream invalidated: resume token is no longer valid, re-backfill required")
	case mongo.OpRename, mongo.OpDrop:
		return Action{Type: ActionSkip}, nil
	default:
		return Action{Type: ActionSkip}, nil
	}
}

func MapDocument(doc map[string]any, clusterTime uint64, coll config.CollectionConfig) (Action, error) {
	docID, err := SerializeID(doc["_id"])
	if err != nil {
		return Action{}, err
	}

	attrs, err := extractFields(doc, coll.Mapping.Fields)
	if err != nil {
		return Action{}, err
	}

	return Action{
		Type:        ActionUpsert,
		DocumentID:  docID,
		Attributes:  attrs,
		ClusterTime: clusterTime,
	}, nil
}

func mapUpsert(event mongo.ChangeEvent, coll config.CollectionConfig) (Action, error) {
	docID, err := SerializeID(event.DocumentID)
	if err != nil {
		return Action{}, err
	}

	attrs, err := extractFields(event.FullDocument, coll.Mapping.Fields)
	if err != nil {
		return Action{}, err
	}

	return Action{
		Type:        ActionUpsert,
		DocumentID:  docID,
		Attributes:  attrs,
		ClusterTime: event.ClusterTime,
	}, nil
}

func mapPatch(event mongo.ChangeEvent, coll config.CollectionConfig) (Action, error) {
	docID, err := SerializeID(event.DocumentID)
	if err != nil {
		return Action{}, err
	}

	attrs, err := extractFields(event.UpdatedFields, coll.Mapping.Fields)
	if err != nil {
		return Action{}, err
	}

	// Removed fields are explicitly nulled in turbopuffer.
	for _, name := range event.RemovedFields {
		for _, f := range coll.Mapping.Fields {
			if f.Name == name {
				attrs[name] = nil
				break
			}
		}
	}

	if len(attrs) == 0 {
		return Action{Type: ActionSkip}, nil
	}

	return Action{
		Type:        ActionPatch,
		DocumentID:  docID,
		Attributes:  attrs,
		ClusterTime: event.ClusterTime,
	}, nil
}

func mapDelete(event mongo.ChangeEvent, coll config.CollectionConfig) (Action, error) {
	if !coll.MirrorDeletesEnabled() {
		return Action{Type: ActionSkip}, nil
	}

	docID, err := SerializeID(event.DocumentID)
	if err != nil {
		return Action{}, err
	}

	return Action{
		Type:        ActionDelete,
		DocumentID:  docID,
		ClusterTime: event.ClusterTime,
	}, nil
}

func extractFields(doc map[string]any, fields []config.FieldMapping) (map[string]any, error) {
	attrs := make(map[string]any, len(fields))
	for _, f := range fields {
		val, exists := doc[f.Name]
		if !exists {
			continue
		}
		coerced, err := coerceValue(val, f)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}
		attrs[f.Name] = coerced
	}
	return attrs, nil
}

// coerceValue converts a BSON value to the expected type per the field mapping.
// Numeric coercion rules:
//   - float: int32/int64 → float64
//   - int: int32 → int64, float64 → int64 (if no fractional part)
//   - uint: int32/int64 → uint64 (reject negatives), float64 → uint64 (if no fractional part, non-negative)
//
// Array variants apply the same rules per element.
// nil (BSON null) passes through for all types.
func coerceValue(val any, f config.FieldMapping) (any, error) {
	if val == nil {
		return nil, nil
	}

	switch f.Type {
	case config.FieldTypeFloat:
		return coerceFloat(val)
	case config.FieldTypeInt:
		return coerceInt(val)
	case config.FieldTypeUint:
		return coerceUint(val)
	case config.FieldTypeFloatArray:
		return coerceArray(val, coerceFloat)
	case config.FieldTypeIntArray:
		return coerceArray(val, coerceInt)
	case config.FieldTypeUintArray:
		return coerceArray(val, coerceUint)
	default:
		return val, nil
	}
}

func coerceFloat(val any) (any, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	default:
		return nil, fmt.Errorf("type mismatch: cannot coerce %T to float", val)
	}
}

func coerceInt(val any) (any, error) {
	switch v := val.(type) {
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		if v != math.Trunc(v) {
			return nil, fmt.Errorf("type mismatch: float64 %v has fractional part, cannot coerce to int", v)
		}
		return int64(v), nil
	default:
		return nil, fmt.Errorf("type mismatch: cannot coerce %T to int", val)
	}
}

func coerceUint(val any) (any, error) {
	switch v := val.(type) {
	case int32:
		if v < 0 {
			return nil, fmt.Errorf("type mismatch: negative value %d cannot be coerced to uint", v)
		}
		return uint64(v), nil
	case int64:
		if v < 0 {
			return nil, fmt.Errorf("type mismatch: negative value %d cannot be coerced to uint", v)
		}
		return uint64(v), nil
	case float64:
		if v != math.Trunc(v) {
			return nil, fmt.Errorf("type mismatch: float64 %v has fractional part, cannot coerce to uint", v)
		}
		if v < 0 {
			return nil, fmt.Errorf("type mismatch: negative value %v cannot be coerced to uint", v)
		}
		return uint64(v), nil
	default:
		return nil, fmt.Errorf("type mismatch: cannot coerce %T to uint", val)
	}
}

func coerceArray(val any, coerceFn func(any) (any, error)) (any, error) {
	arr, ok := val.(bson.A)
	if !ok {
		return nil, fmt.Errorf("type mismatch: expected array, got %T", val)
	}
	result := make([]any, len(arr))
	for i, elem := range arr {
		coerced, err := coerceFn(elem)
		if err != nil {
			return nil, fmt.Errorf("element [%d]: %w", i, err)
		}
		result[i] = coerced
	}
	return result, nil
}
