package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidate_Minimal(t *testing.T) {
	cfg := &AppConfig{
		Collections: []CollectionConfig{
			{
				Name: "orders",
				Mapping: MappingConfig{
					Namespace: MustParseNamespace("orders"),
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
					Namespace: MustParseNamespace("order_data"),
					Fields:    []FieldMapping{{Name: "total", Type: FieldTypeString}},
				},
			},
		},
	}

	if err := Validate(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := cfg.Collections[0].Mapping.Namespace.String(); got != "order_data" {
		t.Errorf("got namespace %q, want %q", got, "order_data")
	}
}

func TestValidate_AllFieldTypes(t *testing.T) {
	cfg := &AppConfig{
		Collections: []CollectionConfig{
			{
				Name: "everything",
				Mapping: MappingConfig{
					Namespace: MustParseNamespace("everything"),
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
					Namespace: MustParseNamespace("users"),
					Fields:    []FieldMapping{{Name: "name", Type: FieldTypeString}},
				},
			},
			{
				Name: "users_v2",
				Mapping: MappingConfig{
					Namespace: MustParseNamespace("users"),
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
					Namespace: MustParseNamespace("products"),
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
					Namespace: MustParseNamespace("docs"),
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
					Namespace: MustParseNamespace("docs"),
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
					Namespace: MustParseNamespace("docs"),
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
	if eff.BatchFlushTimeMs != DefaultBatchFlushTimeMs {
		t.Errorf("got flush time %d, want %d (default)", eff.BatchFlushTimeMs, DefaultBatchFlushTimeMs)
	}
}

// writeConfigFile writes data to a config file in a new temporary directory
// and returns its path.
func writeConfigFile(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mongopuff.yaml")
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

// The loader fills in the default, so a config without a namespace passes
// Validate. Moving the loader once dropped it without a failing test.
func TestLoadConfigFile_NamespaceDefaultsToCollectionName(t *testing.T) {
	path := writeConfigFile(t, `
collections:
  - name: orders
    mapping:
      fields:
        - name: total
          type: float
  - name: users
    mapping:
      namespace: user_data
      fields:
        - name: email
          type: string
`)

	collections, _, err := LoadConfigFile(path)
	if err != nil {
		t.Fatalf("LoadConfigFile: %v", err)
	}

	want := map[string]string{"orders": "orders", "users": "user_data"}
	for _, c := range collections {
		if got := c.Mapping.Namespace.String(); got != want[c.Name] {
			t.Errorf("collection %q: got namespace %q, want %q", c.Name, got, want[c.Name])
		}
	}
}

// MongoDB allows collection names that turbopuffer rejects as namespaces. The
// default must fail at load like a configured name, not on every write.
func TestLoadConfigFile_InvalidDefaultNamespaceFails(t *testing.T) {
	path := writeConfigFile(t, `
collections:
  - name: my orders
    mapping:
      fields:
        - name: total
          type: float
`)

	_, _, err := LoadConfigFile(path)
	if err == nil {
		t.Fatal("expected an error for a collection name that is not a valid namespace")
	}
	if !strings.Contains(err.Error(), `"my orders"`) {
		t.Errorf("error %q should name the collection", err)
	}
}
