package config

import (
	"strings"
	"testing"
)

func TestValidate_Minimal(t *testing.T) {
	cfg := &AppConfig{
		Collections: []CollectionConfig{
			{
				Name: "orders",
				Mapping: MappingConfig{
					Namespace: "orders",
					Fields:    []FieldMapping{{Name: "total", Type: FieldTypeString}},
				},
			},
		},
	}

	if err := Validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_CustomNamespace(t *testing.T) {
	cfg := &AppConfig{
		Collections: []CollectionConfig{
			{
				Name: "orders",
				Mapping: MappingConfig{
					Namespace: "order_data",
					Fields:    []FieldMapping{{Name: "total", Type: FieldTypeString}},
				},
			},
		},
	}

	if err := Validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Collections[0].Mapping.Namespace != "order_data" {
		t.Errorf("got namespace %q, want %q", cfg.Collections[0].Mapping.Namespace, "order_data")
	}
}

func TestValidate_AllFieldTypes(t *testing.T) {
	cfg := &AppConfig{
		Collections: []CollectionConfig{
			{
				Name: "everything",
				Mapping: MappingConfig{
					Namespace: "everything",
					Fields: []FieldMapping{
						{Name: "f_string", Type: FieldTypeString},
						{Name: "f_int", Type: FieldTypeInt},
						{Name: "f_uint", Type: FieldTypeUint},
						{Name: "f_float", Type: FieldTypeFloat},
						{Name: "f_bool", Type: FieldTypeBool},
						{Name: "f_uuid", Type: FieldTypeUUID},
						{Name: "f_datetime", Type: FieldTypeDatetime},
						{Name: "f_strings", Type: FieldTypeStringArray},
						{Name: "f_ints", Type: FieldTypeIntArray},
						{Name: "f_uints", Type: FieldTypeUintArray},
						{Name: "f_floats", Type: FieldTypeFloatArray},
						{Name: "f_bools", Type: FieldTypeBoolArray},
						{Name: "f_uuids", Type: FieldTypeUUIDArray},
						{Name: "f_datetimes", Type: FieldTypeDatetimeArray},
						{Name: "f_vec", Type: FieldTypeVector, Dimension: 128, Precision: VectorPrecisionF32},
					},
				},
			},
		},
	}

	if err := Validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_DuplicateNamespace(t *testing.T) {
	cfg := &AppConfig{
		Collections: []CollectionConfig{
			{
				Name: "users_v1",
				Mapping: MappingConfig{
					Namespace: "users",
					Fields:    []FieldMapping{{Name: "name", Type: FieldTypeString}},
				},
			},
			{
				Name: "users_v2",
				Mapping: MappingConfig{
					Namespace: "users",
					Fields:    []FieldMapping{{Name: "name", Type: FieldTypeString}},
				},
			},
		},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected error for duplicate namespace")
	}
	if !strings.Contains(err.Error(), "users") {
		t.Errorf("error %q should mention the duplicate namespace", err.Error())
	}
}

func TestValidate_EmptyNamespace(t *testing.T) {
	cfg := &AppConfig{
		Collections: []CollectionConfig{
			{
				Name: "foo",
				Mapping: MappingConfig{
					Fields: []FieldMapping{{Name: "a", Type: FieldTypeString}},
				},
			},
		},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected error for empty namespace (should have been resolved by config loader)")
	}
	if !strings.Contains(err.Error(), "foo") {
		t.Errorf("error %q should mention the collection name", err.Error())
	}
}

func TestValidate_UnknownFieldType(t *testing.T) {
	cfg := &AppConfig{
		Collections: []CollectionConfig{
			{
				Name: "products",
				Mapping: MappingConfig{
					Namespace: "products",
					Fields:    []FieldMapping{{Name: "meta", Type: "map"}},
				},
			},
		},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected error for unknown field type")
	}
	if !strings.Contains(err.Error(), "map") {
		t.Errorf("error %q should mention the bad type", err.Error())
	}
	if !strings.Contains(err.Error(), "meta") {
		t.Errorf("error %q should mention the field name", err.Error())
	}
}

func TestValidate_VectorMissingDimension(t *testing.T) {
	cfg := &AppConfig{
		Collections: []CollectionConfig{
			{
				Name: "docs",
				Mapping: MappingConfig{
					Namespace: "docs",
					Fields: []FieldMapping{
						{Name: "embedding", Type: FieldTypeVector, Precision: VectorPrecisionF32},
					},
				},
			},
		},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected error for vector missing dimension")
	}
	if !strings.Contains(err.Error(), "dimension") {
		t.Errorf("error %q should mention dimension", err.Error())
	}
}

func TestValidate_VectorMissingPrecision(t *testing.T) {
	cfg := &AppConfig{
		Collections: []CollectionConfig{
			{
				Name: "docs",
				Mapping: MappingConfig{
					Namespace: "docs",
					Fields: []FieldMapping{
						{Name: "embedding", Type: FieldTypeVector, Dimension: 128},
					},
				},
			},
		},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected error for vector missing precision")
	}
	if !strings.Contains(err.Error(), "precision") {
		t.Errorf("error %q should mention precision", err.Error())
	}
}

func TestValidate_VectorBadPrecision(t *testing.T) {
	cfg := &AppConfig{
		Collections: []CollectionConfig{
			{
				Name: "docs",
				Mapping: MappingConfig{
					Namespace: "docs",
					Fields: []FieldMapping{
						{Name: "embedding", Type: FieldTypeVector, Dimension: 128, Precision: "f64"},
					},
				},
			},
		},
	}

	err := Validate(cfg)
	if err == nil {
		t.Fatal("expected error for bad vector precision")
	}
	if !strings.Contains(err.Error(), "f64") {
		t.Errorf("error %q should mention the bad precision value", err.Error())
	}
}

func TestValidate_GlobalDefaults(t *testing.T) {
	gc := GlobalConfig{}
	eff := gc.Effective()

	if eff.BatchFlushCount != DefaultBatchFlushCount {
		t.Errorf("got flush count %d, want %d", eff.BatchFlushCount, DefaultBatchFlushCount)
	}
	if eff.BatchFlushSize != DefaultBatchFlushSize {
		t.Errorf("got flush size %d, want %d", eff.BatchFlushSize, DefaultBatchFlushSize)
	}
	if eff.BatchFlushTimeMs != DefaultBatchFlushTimeMs {
		t.Errorf("got flush time %d, want %d", eff.BatchFlushTimeMs, DefaultBatchFlushTimeMs)
	}
}

func TestValidate_GlobalOverrides(t *testing.T) {
	gc := GlobalConfig{BatchFlushCount: 512}
	eff := gc.Effective()

	if eff.BatchFlushCount != 512 {
		t.Errorf("got flush count %d, want 512", eff.BatchFlushCount)
	}
	if eff.BatchFlushSize != DefaultBatchFlushSize {
		t.Errorf("got flush size %d, want %d (default)", eff.BatchFlushSize, DefaultBatchFlushSize)
	}
	if eff.BatchFlushTimeMs != DefaultBatchFlushTimeMs {
		t.Errorf("got flush time %d, want %d (default)", eff.BatchFlushTimeMs, DefaultBatchFlushTimeMs)
	}
}
