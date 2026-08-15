package config

import "fmt"

// Validate performs startup validation on the loaded config.
// Note: connection string validation (database name) is handled by
// mongo.ParseDatabaseName at the call site, not here.
func Validate(cfg *AppConfig) error {
	if err := validateUniqueNamespaces(cfg.Collections); err != nil {
		return err
	}

	for _, coll := range cfg.Collections {
		if err := validateCollectionFields(coll); err != nil {
			return fmt.Errorf("collection %q: %w", coll.Name, err)
		}
	}

	return nil
}

func validateUniqueNamespaces(collections []CollectionConfig) error {
	seen := make(map[string]string) // namespace → collection name
	for _, c := range collections {
		if c.Mapping.Namespace == "" {
			return fmt.Errorf("collection %q has no namespace (should have been resolved by the config loader)", c.Name)
		}
		if other, exists := seen[c.Mapping.Namespace]; exists {
			return fmt.Errorf("collections %q and %q both map to namespace %q", other, c.Name, c.Mapping.Namespace)
		}
		seen[c.Mapping.Namespace] = c.Name
	}
	return nil
}

var validFieldTypes = map[FieldType]bool{
	FieldTypeString:        true,
	FieldTypeInt:           true,
	FieldTypeUint:          true,
	FieldTypeFloat:         true,
	FieldTypeBool:          true,
	FieldTypeUUID:          true,
	FieldTypeDatetime:      true,
	FieldTypeStringArray:   true,
	FieldTypeIntArray:      true,
	FieldTypeUintArray:     true,
	FieldTypeFloatArray:    true,
	FieldTypeBoolArray:     true,
	FieldTypeUUIDArray:     true,
	FieldTypeDatetimeArray: true,
	FieldTypeVector:        true,
}

var validPrecisions = map[VectorPrecision]bool{
	VectorPrecisionF32: true,
	VectorPrecisionF16: true,
	VectorPrecisionI8:  true,
}

func validateCollectionFields(coll CollectionConfig) error {
	for _, f := range coll.Mapping.Fields {
		if !validFieldTypes[f.Type] {
			return fmt.Errorf("field %q has unsupported type %q", f.Name, f.Type)
		}
		if f.Type == FieldTypeVector {
			if f.Dimension <= 0 {
				return fmt.Errorf("vector field %q must specify a dimension", f.Name)
			}
			if !validPrecisions[f.Precision] {
				return fmt.Errorf("vector field %q has unsupported precision %q (must be f32, f16, or i8)", f.Name, f.Precision)
			}
		}
	}
	return nil
}
