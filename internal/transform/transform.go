package transform

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/mongo"
)

type ActionType int

const (
	ActionUpsert ActionType = iota
	ActionPatch
	ActionDelete
)

// Action is what gets sent to turbopuffer after transforming a change event.
type Action struct {
	Type        ActionType
	Operation   mongo.Operation // original MongoDB operation (insert, update, replace, delete)
	Namespace   string
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

// MapChangeEvent converts a change stream event into the turbopuffer Actions
// it requires, according to the collection's field mapping config. An event
// that needs no write produces no Actions, a namespace transition produces a
// delete from the old namespace followed by an upsert into the new one, and
// every other event produces one Action.
func MapChangeEvent(event mongo.ChangeEvent, coll config.CollectionConfig) ([]Action, error) {
	switch event.Operation {
	case mongo.OpInsert, mongo.OpReplace:
		action, err := mapUpsert(event, coll)
		if err != nil {
			return nil, err
		}
		return []Action{action}, nil
	case mongo.OpUpdate:
		return mapUpdate(event, coll)
	case mongo.OpDelete:
		return mapDelete(event, coll)
	case mongo.OpInvalidate:
		return nil, fmt.Errorf("change stream invalidated: resume token is no longer valid, re-backfill required")
	default:
		// Rename, drop, and any operation not handled above need no write.
		return nil, nil
	}
}

func MapDocument(doc map[string]any, clusterTime uint64, coll config.CollectionConfig) (Action, error) {
	docID, err := SerializeID(doc["_id"])
	if err != nil {
		return Action{}, err
	}

	ns, err := resolveNamespace(coll.Mapping.Namespace, doc)
	if err != nil {
		return Action{}, err
	}

	attrs, err := extractFields(doc, coll.Mapping.Fields)
	if err != nil {
		return Action{}, err
	}

	return Action{
		Type:        ActionUpsert,
		Operation:   mongo.OpInsert,
		Namespace:   ns,
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

	ns, err := resolveNamespace(coll.Mapping.Namespace, event.FullDocument)
	if err != nil {
		return Action{}, err
	}

	attrs, err := extractFields(event.FullDocument, coll.Mapping.Fields)
	if err != nil {
		return Action{}, err
	}

	return Action{
		Type:        ActionUpsert,
		Namespace:   ns,
		Operation:   event.Operation,
		DocumentID:  docID,
		Attributes:  attrs,
		ClusterTime: event.ClusterTime,
	}, nil
}

// mapUpdate handles OpUpdate events. When the namespace is dynamic and a pre-image
// is available, it checks whether the namespace field changed. If it did, a namespace
// transition is detected and two actions are returned: a delete from the old namespace
// followed by an upsert into the new namespace. Otherwise the event is a normal patch.
func mapUpdate(event mongo.ChangeEvent, coll config.CollectionConfig) ([]Action, error) {
	if coll.Mapping.Namespace.IsDynamic() && event.FullDocumentBeforeChange != nil {
		oldNS, err := resolveNamespace(coll.Mapping.Namespace, event.FullDocumentBeforeChange)
		if err != nil {
			return nil, err
		}
		newNS, err := resolveNamespace(coll.Mapping.Namespace, event.FullDocument)
		if err != nil {
			return nil, err
		}
		if oldNS != newNS {
			return mapNamespaceTransition(event, coll, oldNS, newNS)
		}
	}

	return mapPatch(event, coll)
}

func mapNamespaceTransition(event mongo.ChangeEvent, coll config.CollectionConfig, oldNS, newNS string) ([]Action, error) {
	docID, err := SerializeID(event.DocumentID)
	if err != nil {
		return nil, err
	}

	attrs, err := extractFields(event.FullDocument, coll.Mapping.Fields)
	if err != nil {
		return nil, err
	}

	del := Action{
		Type:        ActionDelete,
		Namespace:   oldNS,
		Operation:   event.Operation,
		DocumentID:  docID,
		ClusterTime: event.ClusterTime,
	}
	ups := Action{
		Type:        ActionUpsert,
		Namespace:   newNS,
		Operation:   event.Operation,
		DocumentID:  docID,
		Attributes:  attrs,
		ClusterTime: event.ClusterTime,
	}
	return []Action{del, ups}, nil
}

func mapPatch(event mongo.ChangeEvent, coll config.CollectionConfig) ([]Action, error) {
	docID, err := SerializeID(event.DocumentID)
	if err != nil {
		return nil, err
	}

	attrs, err := extractFields(event.UpdatedFields, coll.Mapping.Fields)
	if err != nil {
		return nil, err
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
		return nil, nil
	}

	ns, err := resolveNamespace(coll.Mapping.Namespace, event.FullDocument)
	if err != nil {
		return nil, err
	}

	return []Action{{
		Type:        ActionPatch,
		Namespace:   ns,
		Operation:   event.Operation,
		DocumentID:  docID,
		Attributes:  attrs,
		ClusterTime: event.ClusterTime,
	}}, nil
}

func mapDelete(event mongo.ChangeEvent, coll config.CollectionConfig) ([]Action, error) {
	if !coll.MirrorDeletesEnabled() {
		return nil, nil
	}

	docID, err := SerializeID(event.DocumentID)
	if err != nil {
		return nil, err
	}

	ns, err := resolveNamespace(coll.Mapping.Namespace, event.FullDocumentBeforeChange)
	if err != nil {
		return nil, err
	}

	return []Action{{
		Type:        ActionDelete,
		Namespace:   ns,
		Operation:   event.Operation,
		DocumentID:  docID,
		ClusterTime: event.ClusterTime,
	}}, nil
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

// coerceValue converts and validates a BSON value per the field mapping.
// nil (BSON null) passes through for all types.
func coerceValue(val any, f config.FieldMapping) (any, error) {
	if val == nil {
		return nil, nil
	}

	switch f.Type {
	case config.FieldTypeString:
		return coerceString(val)
	case config.FieldTypeBool:
		return coerceBool(val)
	case config.FieldTypeFloat:
		return coerceFloat(val)
	case config.FieldTypeInt:
		return coerceInt(val)
	case config.FieldTypeUint:
		return coerceUint(val)
	case config.FieldTypeUUID:
		return coerceUUID(val)
	case config.FieldTypeDatetime:
		return coerceDatetime(val)
	case config.FieldTypeVector:
		return coerceVector(val, f.Dimension)
	case config.FieldTypeStringArray:
		return coerceArray(val, coerceString)
	case config.FieldTypeBoolArray:
		return coerceArray(val, coerceBool)
	case config.FieldTypeFloatArray:
		return coerceArray(val, coerceFloat)
	case config.FieldTypeIntArray:
		return coerceArray(val, coerceInt)
	case config.FieldTypeUintArray:
		return coerceArray(val, coerceUint)
	case config.FieldTypeUUIDArray:
		return coerceArray(val, coerceUUID)
	case config.FieldTypeDatetimeArray:
		return coerceArray(val, coerceDatetime)
	default:
		return nil, fmt.Errorf("unsupported field type %q", f.Type)
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

func coerceString(val any) (any, error) {
	if _, ok := val.(string); !ok {
		return nil, fmt.Errorf("type mismatch: expected string, got %T", val)
	}
	return val, nil
}

func coerceBool(val any) (any, error) {
	if _, ok := val.(bool); !ok {
		return nil, fmt.Errorf("type mismatch: expected bool, got %T", val)
	}
	return val, nil
}

func coerceUUID(val any) (any, error) {
	v, ok := val.(bson.Binary)
	if !ok {
		return nil, fmt.Errorf("type mismatch: expected Binary, got %T", val)
	}
	if v.Subtype != bson.TypeBinaryUUID {
		return nil, fmt.Errorf("type mismatch: expected UUID binary subtype, got 0x%02x", v.Subtype)
	}
	if len(v.Data) != 16 {
		return nil, fmt.Errorf("type mismatch: UUID binary has invalid length %d, expected 16", len(v.Data))
	}
	return formatUUID(v.Data), nil
}

func coerceDatetime(val any) (any, error) {
	if _, ok := val.(time.Time); !ok {
		return nil, fmt.Errorf("type mismatch: expected datetime, got %T", val)
	}
	return val, nil
}

func coerceVector(val any, dimension int) (any, error) {
	arr, ok := val.(bson.A)
	if !ok {
		return nil, fmt.Errorf("type mismatch: expected array for vector, got %T", val)
	}
	if len(arr) != dimension {
		return nil, fmt.Errorf("type mismatch: vector dimension %d does not match expected %d", len(arr), dimension)
	}
	result := make([]float32, len(arr))
	for i, elem := range arr {
		switch v := elem.(type) {
		case float64:
			result[i] = float32(v)
		case int32:
			result[i] = float32(v)
		case int64:
			result[i] = float32(v)
		default:
			return nil, fmt.Errorf("element [%d]: type mismatch: expected numeric, got %T", i, elem)
		}
	}
	return result, nil
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
