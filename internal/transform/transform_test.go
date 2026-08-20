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

// --- Numeric Coercion ---

func TestCoerceValue_FloatFromInt32(t *testing.T) {
	f := config.FieldMapping{Name: "calories", Type: config.FieldTypeFloat}
	got, err := coerceValue(int32(42), f)
	if err != nil {
		t.Fatal(err)
	}
	if got != float64(42) {
		t.Errorf("got %v (%T), want float64(42)", got, got)
	}
}

func TestCoerceValue_FloatFromInt64(t *testing.T) {
	f := config.FieldMapping{Name: "calories", Type: config.FieldTypeFloat}
	got, err := coerceValue(int64(1000000), f)
	if err != nil {
		t.Fatal(err)
	}
	if got != float64(1000000) {
		t.Errorf("got %v (%T), want float64(1000000)", got, got)
	}
}

func TestCoerceValue_FloatPassthrough(t *testing.T) {
	f := config.FieldMapping{Name: "calories", Type: config.FieldTypeFloat}
	got, err := coerceValue(float64(3.14), f)
	if err != nil {
		t.Fatal(err)
	}
	if got != float64(3.14) {
		t.Errorf("got %v, want 3.14", got)
	}
}

func TestCoerceValue_FloatTypeMismatch(t *testing.T) {
	f := config.FieldMapping{Name: "calories", Type: config.FieldTypeFloat}
	_, err := coerceValue("not a number", f)
	if err == nil {
		t.Fatal("expected type mismatch error")
	}
}

func TestCoerceValue_IntFromFloat64Whole(t *testing.T) {
	f := config.FieldMapping{Name: "count", Type: config.FieldTypeInt}
	got, err := coerceValue(float64(42.0), f)
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(42) {
		t.Errorf("got %v (%T), want int64(42)", got, got)
	}
}

func TestCoerceValue_IntFromFloat64Fractional(t *testing.T) {
	f := config.FieldMapping{Name: "count", Type: config.FieldTypeInt}
	_, err := coerceValue(float64(42.5), f)
	if err == nil {
		t.Fatal("expected type mismatch error for fractional float")
	}
}

func TestCoerceValue_IntFromInt32(t *testing.T) {
	f := config.FieldMapping{Name: "count", Type: config.FieldTypeInt}
	got, err := coerceValue(int32(7), f)
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(7) {
		t.Errorf("got %v (%T), want int64(7)", got, got)
	}
}

func TestCoerceValue_UintRejectsNegativeInt32(t *testing.T) {
	f := config.FieldMapping{Name: "count", Type: config.FieldTypeUint}
	_, err := coerceValue(int32(-1), f)
	if err == nil {
		t.Fatal("expected error for negative value coerced to uint")
	}
}

func TestCoerceValue_UintRejectsNegativeInt64(t *testing.T) {
	f := config.FieldMapping{Name: "count", Type: config.FieldTypeUint}
	_, err := coerceValue(int64(-5), f)
	if err == nil {
		t.Fatal("expected error for negative value coerced to uint")
	}
}

func TestCoerceValue_UintFromFloat64(t *testing.T) {
	f := config.FieldMapping{Name: "count", Type: config.FieldTypeUint}
	got, err := coerceValue(float64(100.0), f)
	if err != nil {
		t.Fatal(err)
	}
	if got != uint64(100) {
		t.Errorf("got %v (%T), want uint64(100)", got, got)
	}
}

func TestCoerceValue_UintRejectsNegativeFloat64(t *testing.T) {
	f := config.FieldMapping{Name: "count", Type: config.FieldTypeUint}
	_, err := coerceValue(float64(-5.0), f)
	if err == nil {
		t.Fatal("expected error for negative float coerced to uint")
	}
}

func TestCoerceValue_UintRejectsFractionalFloat64(t *testing.T) {
	f := config.FieldMapping{Name: "count", Type: config.FieldTypeUint}
	_, err := coerceValue(float64(100.5), f)
	if err == nil {
		t.Fatal("expected error for fractional float coerced to uint")
	}
}

func TestCoerceValue_IntFromInt64(t *testing.T) {
	f := config.FieldMapping{Name: "count", Type: config.FieldTypeInt}
	got, err := coerceValue(int64(9223372036854775807), f)
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(9223372036854775807) {
		t.Errorf("got %v (%T), want int64(max)", got, got)
	}
}

func TestCoerceValue_Null(t *testing.T) {
	f := config.FieldMapping{Name: "calories", Type: config.FieldTypeFloat}
	got, err := coerceValue(nil, f)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestCoerceValue_FloatArrayMixed(t *testing.T) {
	f := config.FieldMapping{Name: "scores", Type: config.FieldTypeFloatArray}
	input := bson.A{int32(1), float64(2.5), int64(3)}
	got, err := coerceValue(input, f)
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
	for i, want := range []float64{1.0, 2.5, 3.0} {
		if arr[i] != want {
			t.Errorf("element [%d]: got %v (%T), want %v", i, arr[i], arr[i], want)
		}
	}
}

func TestCoerceValue_IntArrayBadElement(t *testing.T) {
	f := config.FieldMapping{Name: "ids", Type: config.FieldTypeIntArray}
	input := bson.A{int32(1), float64(2.5)}
	_, err := coerceValue(input, f)
	if err == nil {
		t.Fatal("expected error for fractional element in int array")
	}
}

func TestCoerceValue_NonNumericPassthrough(t *testing.T) {
	f := config.FieldMapping{Name: "name", Type: config.FieldTypeString}
	got, err := coerceValue("hello", f)
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Errorf("got %v, want %q", got, "hello")
	}
}

// --- UUID Coercion ---

func TestCoerceValue_UUID(t *testing.T) {
	f := config.FieldMapping{Name: "userId", Type: config.FieldTypeUUID}
	input := bson.Binary{
		Subtype: bson.TypeBinaryUUID,
		Data: []byte{
			0x55, 0x0e, 0x84, 0x00,
			0xe2, 0x9b,
			0x41, 0xd4,
			0xa7, 0x16,
			0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
		},
	}
	got, err := coerceValue(input, f)
	if err != nil {
		t.Fatal(err)
	}
	want := "550e8400-e29b-41d4-a716-446655440000"
	if got != want {
		t.Errorf("got %v, want %q", got, want)
	}
}

func TestCoerceValue_UUIDNonUUIDSubtype(t *testing.T) {
	f := config.FieldMapping{Name: "userId", Type: config.FieldTypeUUID}
	input := bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: make([]byte, 16)}
	_, err := coerceValue(input, f)
	if err == nil {
		t.Fatal("expected error for non-UUID binary subtype")
	}
}

func TestCoerceValue_UUIDWrongLength(t *testing.T) {
	f := config.FieldMapping{Name: "userId", Type: config.FieldTypeUUID}
	input := bson.Binary{Subtype: bson.TypeBinaryUUID, Data: []byte{0x01, 0x02, 0x03}}
	_, err := coerceValue(input, f)
	if err == nil {
		t.Fatal("expected error for UUID with wrong data length")
	}
}

func TestCoerceValue_UUIDTypeMismatch(t *testing.T) {
	f := config.FieldMapping{Name: "userId", Type: config.FieldTypeUUID}
	_, err := coerceValue("not-a-binary", f)
	if err == nil {
		t.Fatal("expected error for string value on uuid field")
	}
}

func TestCoerceValue_UUIDArray(t *testing.T) {
	f := config.FieldMapping{Name: "userIds", Type: config.FieldTypeUUIDArray}
	uuid1 := bson.Binary{
		Subtype: bson.TypeBinaryUUID,
		Data: []byte{
			0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
		},
	}
	uuid2 := bson.Binary{
		Subtype: bson.TypeBinaryUUID,
		Data: []byte{
			0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
			0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
		},
	}
	got, err := coerceValue(bson.A{uuid1, uuid2}, f)
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(arr) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(arr))
	}
	if arr[0] != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("element [0]: got %v", arr[0])
	}
	if arr[1] != "12345678-9abc-def0-1234-56789abcdef0" {
		t.Errorf("element [1]: got %v", arr[1])
	}
}

// --- Datetime Coercion ---

func TestCoerceValue_DatetimeTypeMismatch(t *testing.T) {
	f := config.FieldMapping{Name: "createdAt", Type: config.FieldTypeDatetime}
	_, err := coerceValue("2025-08-01", f)
	if err == nil {
		t.Fatal("expected error for string value on datetime field")
	}
}

// --- Vector Coercion ---

func TestCoerceValue_VectorF32(t *testing.T) {
	f := config.FieldMapping{Name: "embedding", Type: config.FieldTypeVector, Dimension: 4, Precision: config.VectorPrecisionF32}
	input := bson.A{float64(1.0), float64(2.0), float64(3.0), float64(4.0)}
	got, err := coerceValue(input, f)
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := got.([]float32)
	if !ok {
		t.Fatalf("expected []float32, got %T", got)
	}
	if len(arr) != 4 {
		t.Fatalf("expected 4 elements, got %d", len(arr))
	}
	for i, want := range []float32{1.0, 2.0, 3.0, 4.0} {
		if arr[i] != want {
			t.Errorf("element [%d]: got %v, want %v", i, arr[i], want)
		}
	}
}

func TestCoerceValue_VectorF16(t *testing.T) {
	// f16 precision still produces []float32 — turbopuffer quantizes server-side.
	f := config.FieldMapping{Name: "embedding", Type: config.FieldTypeVector, Dimension: 3, Precision: config.VectorPrecisionF16}
	input := bson.A{float64(0.5), float64(1.5), float64(2.5)}
	got, err := coerceValue(input, f)
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := got.([]float32)
	if !ok {
		t.Fatalf("expected []float32, got %T", got)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
}

func TestCoerceValue_VectorI8(t *testing.T) {
	// i8 precision still produces []float32 — turbopuffer quantizes server-side.
	f := config.FieldMapping{Name: "embedding", Type: config.FieldTypeVector, Dimension: 3, Precision: config.VectorPrecisionI8}
	input := bson.A{float64(0.1), float64(0.2), float64(0.3)}
	got, err := coerceValue(input, f)
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := got.([]float32)
	if !ok {
		t.Fatalf("expected []float32, got %T", got)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
}

func TestCoerceValue_VectorWrongDimension(t *testing.T) {
	f := config.FieldMapping{Name: "embedding", Type: config.FieldTypeVector, Dimension: 128, Precision: config.VectorPrecisionF32}
	input := bson.A{float64(1.0), float64(2.0), float64(3.0)} // 3 elements, expected 128
	_, err := coerceValue(input, f)
	if err == nil {
		t.Fatal("expected error for wrong vector dimension")
	}
}

func TestCoerceValue_VectorNotArray(t *testing.T) {
	f := config.FieldMapping{Name: "embedding", Type: config.FieldTypeVector, Dimension: 3, Precision: config.VectorPrecisionF32}
	_, err := coerceValue("not an array", f)
	if err == nil {
		t.Fatal("expected error for non-array value on vector field")
	}
}

func TestCoerceValue_VectorBadElement(t *testing.T) {
	f := config.FieldMapping{Name: "embedding", Type: config.FieldTypeVector, Dimension: 3, Precision: config.VectorPrecisionF32}
	input := bson.A{float64(1.0), "not a number", float64(3.0)}
	_, err := coerceValue(input, f)
	if err == nil {
		t.Fatal("expected error for non-numeric element in vector")
	}
}

func TestCoerceValue_VectorCoercesIntElements(t *testing.T) {
	f := config.FieldMapping{Name: "embedding", Type: config.FieldTypeVector, Dimension: 4, Precision: config.VectorPrecisionF32}
	input := bson.A{int32(1), int64(2), float64(3.5), int32(4)}
	got, err := coerceValue(input, f)
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := got.([]float32)
	if !ok {
		t.Fatalf("expected []float32, got %T", got)
	}
	for i, want := range []float32{1.0, 2.0, 3.5, 4.0} {
		if arr[i] != want {
			t.Errorf("element [%d]: got %v, want %v", i, arr[i], want)
		}
	}
}

// --- String/Bool Array Coercion ---

func TestCoerceValue_StringArrayTypeMismatch(t *testing.T) {
	f := config.FieldMapping{Name: "tags", Type: config.FieldTypeStringArray}
	input := bson.A{"alpha", int32(42)}
	_, err := coerceValue(input, f)
	if err == nil {
		t.Fatal("expected error for non-string element in string array")
	}
}

func TestCoerceValue_BoolArrayTypeMismatch(t *testing.T) {
	f := config.FieldMapping{Name: "flags", Type: config.FieldTypeBoolArray}
	input := bson.A{true, "not a bool"}
	_, err := coerceValue(input, f)
	if err == nil {
		t.Fatal("expected error for non-bool element in bool array")
	}
}

// --- extractFields integration ---

func TestExtractFields_NullValue(t *testing.T) {
	fields := []config.FieldMapping{
		{Name: "name", Type: config.FieldTypeString},
		{Name: "email", Type: config.FieldTypeString},
	}
	doc := map[string]any{"name": "alice", "email": nil}
	attrs, err := extractFields(doc, fields)
	if err != nil {
		t.Fatal(err)
	}
	if attrs["name"] != "alice" {
		t.Errorf("got name %v, want %q", attrs["name"], "alice")
	}
	val, ok := attrs["email"]
	if !ok {
		t.Fatal("expected email in attrs")
	}
	if val != nil {
		t.Errorf("got email %v, want nil", val)
	}
}

func TestExtractFields_TypeMismatchReturnsError(t *testing.T) {
	fields := []config.FieldMapping{
		{Name: "name", Type: config.FieldTypeString},
		{Name: "count", Type: config.FieldTypeInt},
	}
	doc := map[string]any{"name": "alice", "count": "not a number"}
	_, err := extractFields(doc, fields)
	if err == nil {
		t.Fatal("expected error for type mismatch")
	}
}

func TestExtractFields_UnmappedIgnored(t *testing.T) {
	fields := []config.FieldMapping{
		{Name: "name", Type: config.FieldTypeString},
	}
	doc := map[string]any{"name": "alice", "age": 30, "email": "a@b.com"}
	attrs, err := extractFields(doc, fields)
	if err != nil {
		t.Fatal(err)
	}
	if len(attrs) != 1 {
		t.Errorf("expected 1 attribute, got %d: %v", len(attrs), attrs)
	}
	if attrs["name"] != "alice" {
		t.Errorf("got name %v, want %q", attrs["name"], "alice")
	}
}

// Verify patch path uses coercion via extractFields.
func TestMapChangeEvent_PatchCoercesFloat(t *testing.T) {
	oid := mustObjectID("507f1f77bcf86cd799439011")
	fields := []config.FieldMapping{
		{Name: "calories", Type: config.FieldTypeFloat},
	}

	action, err := MapChangeEvent(mongo.ChangeEvent{
		Operation:     mongo.OpUpdate,
		DocumentID:    oid,
		UpdatedFields: map[string]any{"calories": int32(400)},
		ClusterTime:   SerializeClusterTime(10, 1),
	}, testColl(fields, nil))

	if err != nil {
		t.Fatal(err)
	}
	if action.Type != ActionPatch {
		t.Fatalf("got action type %d, want ActionPatch", action.Type)
	}
	v, ok := action.Attributes["calories"]
	if !ok {
		t.Fatal("expected calories in attributes")
	}
	if v != float64(400) {
		t.Errorf("got %v (%T), want float64(400)", v, v)
	}
}
