package transform

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/mongo"
)

func mustObjectID(hex string) bson.ObjectID {
	oid, err := bson.ObjectIDFromHex(hex)
	if err != nil {
		panic(err)
	}
	return oid
}

func TestSerializeID(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    string
		wantErr string // substring to match; empty means no error expected
	}{
		{
			name:  "objectid",
			input: mustObjectID("507f1f77bcf86cd799439011"),
			want:  "507f1f77bcf86cd799439011",
		},
		{
			name:  "string",
			input: "my-custom-id",
			want:  "my-custom-id",
		},
		{
			name:  "int32",
			input: int32(42),
			want:  "42",
		},
		{
			name:  "int32_negative",
			input: int32(-1),
			want:  "-1",
		},
		{
			name:  "int64",
			input: int64(9223372036854775807),
			want:  "9223372036854775807",
		},
		{
			name: "binary_uuid",
			input: bson.Binary{
				Subtype: bson.TypeBinaryUUID,
				Data: []byte{
					0x55, 0x0e, 0x84, 0x00,
					0xe2, 0x9b,
					0x41, 0xd4,
					0xa7, 0x16,
					0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
				},
			},
			want: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:    "binary_non_uuid",
			input:   bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: []byte{0x01, 0x02}},
			wantErr: "subtype",
		},
		{
			name:    "binary_uuid_wrong_length",
			input:   bson.Binary{Subtype: bson.TypeBinaryUUID, Data: []byte{0x01, 0x02, 0x03}},
			wantErr: "length",
		},
		{
			name:    "nil",
			input:   nil,
			wantErr: "nil",
		},
		{
			name:    "unsupported_float64",
			input:   3.14,
			wantErr: "unsupported",
		},
		{
			name:    "unsupported_decimal128",
			input:   bson.Decimal128{},
			wantErr: "unsupported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SerializeID(tt.input)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSerializeClusterTime(t *testing.T) {
	got := SerializeClusterTime(1, 2)
	want := uint64(1)<<32 | uint64(2)
	if got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func TestSerializeClusterTime_Ordering(t *testing.T) {
	t.Run("higher_T", func(t *testing.T) {
		a := SerializeClusterTime(1, 5)
		b := SerializeClusterTime(2, 1)
		if a >= b {
			t.Errorf("T=1,I=5 (%d) should be less than T=2,I=1 (%d)", a, b)
		}
	})

	t.Run("same_T_higher_I", func(t *testing.T) {
		c := SerializeClusterTime(3, 1)
		d := SerializeClusterTime(3, 2)
		if c >= d {
			t.Errorf("T=3,I=1 (%d) should be less than T=3,I=2 (%d)", c, d)
		}
	})
}

func TestMapChangeEvent_IDMissing(t *testing.T) {
	coll := config.CollectionConfig{
		Name: "users",
		Mapping: config.MappingConfig{
			Fields: []config.FieldMapping{{Name: "name", Type: "string"}},
		},
	}

	t.Run("insert", func(t *testing.T) {
		_, err := MapChangeEvent(mongo.ChangeEvent{
			Operation:    mongo.OpInsert,
			DocumentID:   nil,
			FullDocument: map[string]any{"name": "alice"},
			ClusterTime:  SerializeClusterTime(1, 1),
		}, coll)
		if err == nil {
			t.Error("expected error for missing _id on insert event")
		}
	})

	t.Run("update", func(t *testing.T) {
		_, err := MapChangeEvent(mongo.ChangeEvent{
			Operation:     mongo.OpUpdate,
			DocumentID:    nil,
			UpdatedFields: map[string]any{"name": "bob"},
			ClusterTime:   SerializeClusterTime(1, 1),
		}, coll)
		if err == nil {
			t.Error("expected error for missing _id on update event")
		}
	})

	t.Run("delete", func(t *testing.T) {
		_, err := MapChangeEvent(mongo.ChangeEvent{
			Operation:   mongo.OpDelete,
			DocumentID:  nil,
			ClusterTime: SerializeClusterTime(1, 1),
		}, coll)
		if err == nil {
			t.Error("expected error for missing _id on delete event")
		}
	})
}

// --- Change Stream Event Mapping (TESTING.md §4) ---

func testColl(fields []config.FieldMapping, mirrorDeletes *bool) config.CollectionConfig {
	return config.CollectionConfig{
		Name:          "users",
		MirrorDeletes: mirrorDeletes,
		Mapping: config.MappingConfig{
			Fields: fields,
		},
	}
}

func boolPtr(b bool) *bool { return &b }

var testFields = []config.FieldMapping{
	{Name: "name", Type: config.FieldTypeString},
	{Name: "email", Type: config.FieldTypeString},
}

func TestMapChangeEvent_Insert(t *testing.T) {
	oid := mustObjectID("507f1f77bcf86cd799439011")
	ct := SerializeClusterTime(10, 1)

	action, err := MapChangeEvent(mongo.ChangeEvent{
		Operation:    mongo.OpInsert,
		DocumentID:   oid,
		FullDocument: map[string]any{"name": "alice", "email": "a@b.com", "age": 30},
		ClusterTime:  ct,
	}, testColl(testFields, nil))

	if err != nil {
		t.Fatal(err)
	}
	if action.Type != ActionUpsert {
		t.Fatalf("got action type %d, want ActionUpsert", action.Type)
	}
	if action.DocumentID != "507f1f77bcf86cd799439011" {
		t.Errorf("got document ID %q, want %q", action.DocumentID, "507f1f77bcf86cd799439011")
	}
	if action.ClusterTime != ct {
		t.Errorf("got cluster time %d, want %d", action.ClusterTime, ct)
	}
	if action.Attributes["name"] != "alice" {
		t.Errorf("got name %v, want %q", action.Attributes["name"], "alice")
	}
	if action.Attributes["email"] != "a@b.com" {
		t.Errorf("got email %v, want %q", action.Attributes["email"], "a@b.com")
	}
	if _, ok := action.Attributes["age"]; ok {
		t.Error("unmapped field 'age' should not be in attributes")
	}
}

func TestMapChangeEvent_Replace(t *testing.T) {
	oid := mustObjectID("507f1f77bcf86cd799439011")
	ct := SerializeClusterTime(10, 2)

	action, err := MapChangeEvent(mongo.ChangeEvent{
		Operation:    mongo.OpReplace,
		DocumentID:   oid,
		FullDocument: map[string]any{"name": "bob", "email": "b@c.com"},
		ClusterTime:  ct,
	}, testColl(testFields, nil))

	if err != nil {
		t.Fatal(err)
	}
	if action.Type != ActionUpsert {
		t.Fatalf("got action type %d, want ActionUpsert", action.Type)
	}
	if action.ClusterTime != ct {
		t.Errorf("got cluster time %d, want %d", action.ClusterTime, ct)
	}
	if action.Attributes["name"] != "bob" {
		t.Errorf("got name %v, want %q", action.Attributes["name"], "bob")
	}
}

func TestMapChangeEvent_UpdateMappedFields(t *testing.T) {
	oid := mustObjectID("507f1f77bcf86cd799439011")

	action, err := MapChangeEvent(mongo.ChangeEvent{
		Operation:     mongo.OpUpdate,
		DocumentID:    oid,
		UpdatedFields: map[string]any{"name": "carol", "age": 31},
		ClusterTime:   SerializeClusterTime(10, 3),
	}, testColl(testFields, nil))

	if err != nil {
		t.Fatal(err)
	}
	if action.Type != ActionPatch {
		t.Fatalf("got action type %d, want ActionPatch", action.Type)
	}
	if action.Attributes["name"] != "carol" {
		t.Errorf("got name %v, want %q", action.Attributes["name"], "carol")
	}
	if _, ok := action.Attributes["age"]; ok {
		t.Error("unmapped field 'age' should not be in patch attributes")
	}
	if _, ok := action.Attributes["email"]; ok {
		t.Error("unchanged mapped field 'email' should not be in patch attributes")
	}
}

func TestMapChangeEvent_UpdateUnmappedFieldsOnly(t *testing.T) {
	oid := mustObjectID("507f1f77bcf86cd799439011")

	action, err := MapChangeEvent(mongo.ChangeEvent{
		Operation:     mongo.OpUpdate,
		DocumentID:    oid,
		UpdatedFields: map[string]any{"age": 32, "phone": "555-1234"},
		ClusterTime:   SerializeClusterTime(10, 4),
	}, testColl(testFields, nil))

	if err != nil {
		t.Fatal(err)
	}
	if action.Type != ActionSkip {
		t.Errorf("got action type %d, want ActionSkip", action.Type)
	}
}

func TestMapChangeEvent_UpdateFieldRemoved(t *testing.T) {
	oid := mustObjectID("507f1f77bcf86cd799439011")

	action, err := MapChangeEvent(mongo.ChangeEvent{
		Operation:     mongo.OpUpdate,
		DocumentID:    oid,
		RemovedFields: []string{"email"},
		ClusterTime:   SerializeClusterTime(10, 5),
	}, testColl(testFields, nil))

	if err != nil {
		t.Fatal(err)
	}
	if action.Type != ActionPatch {
		t.Fatalf("got action type %d, want ActionPatch", action.Type)
	}
	val, ok := action.Attributes["email"]
	if !ok {
		t.Fatal("removed mapped field 'email' should be in patch attributes")
	}
	if val != nil {
		t.Errorf("removed field should be nil, got %v", val)
	}
}

func TestMapChangeEvent_Delete(t *testing.T) {
	oid := mustObjectID("507f1f77bcf86cd799439011")

	action, err := MapChangeEvent(mongo.ChangeEvent{
		Operation:   mongo.OpDelete,
		DocumentID:  oid,
		ClusterTime: SerializeClusterTime(10, 6),
	}, testColl(testFields, nil))

	if err != nil {
		t.Fatal(err)
	}
	if action.Type != ActionDelete {
		t.Fatalf("got action type %d, want ActionDelete", action.Type)
	}
	if action.DocumentID != "507f1f77bcf86cd799439011" {
		t.Errorf("got document ID %q, want %q", action.DocumentID, "507f1f77bcf86cd799439011")
	}
}

func TestMapChangeEvent_DeleteMirrorDisabled(t *testing.T) {
	oid := mustObjectID("507f1f77bcf86cd799439011")

	action, err := MapChangeEvent(mongo.ChangeEvent{
		Operation:   mongo.OpDelete,
		DocumentID:  oid,
		ClusterTime: SerializeClusterTime(10, 7),
	}, testColl(testFields, boolPtr(false)))

	if err != nil {
		t.Fatal(err)
	}
	if action.Type != ActionSkip {
		t.Errorf("got action type %d, want ActionSkip when mirrorDeletes is false", action.Type)
	}
}

func TestMapChangeEvent_Invalidate(t *testing.T) {
	_, err := MapChangeEvent(mongo.ChangeEvent{
		Operation:   mongo.OpInvalidate,
		ClusterTime: SerializeClusterTime(10, 8),
	}, testColl(testFields, nil))

	if err == nil {
		t.Error("expected error for invalidate event")
	}
}

func TestMapChangeEvent_Rename(t *testing.T) {
	action, err := MapChangeEvent(mongo.ChangeEvent{
		Operation:   mongo.OpRename,
		ClusterTime: SerializeClusterTime(10, 9),
	}, testColl(testFields, nil))

	if err != nil {
		t.Fatal(err)
	}
	if action.Type != ActionSkip {
		t.Errorf("got action type %d, want ActionSkip for rename", action.Type)
	}
}

func TestMapChangeEvent_Drop(t *testing.T) {
	action, err := MapChangeEvent(mongo.ChangeEvent{
		Operation:   mongo.OpDrop,
		ClusterTime: SerializeClusterTime(10, 10),
	}, testColl(testFields, nil))

	if err != nil {
		t.Fatal(err)
	}
	if action.Type != ActionSkip {
		t.Errorf("got action type %d, want ActionSkip for drop", action.Type)
	}
}
