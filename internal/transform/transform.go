package transform

import (
	"encoding/hex"
	"fmt"
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

	mapped := fieldSet(coll.Mapping.Fields)
	attrs := make(map[string]any)

	for name, val := range event.UpdatedFields {
		if _, ok := mapped[name]; ok {
			attrs[name] = val
		}
	}

	for _, name := range event.RemovedFields {
		if _, ok := mapped[name]; ok {
			attrs[name] = nil // explicit null in turbopuffer
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
		// TODO: type validation and conversion per FieldType
		attrs[f.Name] = val
	}
	return attrs, nil
}

func fieldSet(fields []config.FieldMapping) map[string]struct{} {
	s := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		s[f.Name] = struct{}{}
	}
	return s
}
