package transform

import (
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tmccann21/mongopuff/internal/config"
)

func TestResolveNamespace_SingleTokenPresent(t *testing.T) {
	doc := map[string]any{"tenant": "acme"}
	got, err := resolveNamespace(config.MustParseNamespace("products_{{tenant}}"), doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "products_acme" {
		t.Errorf("got %q, want %q", got, "products_acme")
	}
}

func TestResolveNamespace_SingleTokenAbsent(t *testing.T) {
	doc := map[string]any{"other": "value"}
	_, err := resolveNamespace(config.MustParseNamespace("products_{{tenant}}"), doc)
	if err == nil {
		t.Fatal("expected error for absent namespace field, got nil")
	}
	if !strings.Contains(err.Error(), "absent or null") {
		t.Errorf("error %q should mention absent or null", err.Error())
	}
}

func TestResolveNamespace_SingleTokenNull(t *testing.T) {
	doc := map[string]any{"tenant": nil}
	_, err := resolveNamespace(config.MustParseNamespace("products_{{tenant}}"), doc)
	if err == nil {
		t.Fatal("expected error for null namespace field, got nil")
	}
	if !strings.Contains(err.Error(), "absent or null") {
		t.Errorf("error %q should mention absent or null", err.Error())
	}
}

func TestResolveNamespace_MultiTokenAllPresent(t *testing.T) {
	doc := map[string]any{"region": "us", "tier": "gold"}
	got, err := resolveNamespace(config.MustParseNamespace("{{region}}_products_{{tier}}"), doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "us_products_gold" {
		t.Errorf("got %q, want %q", got, "us_products_gold")
	}
}

func TestResolveNamespace_MultiTokenOneAbsent(t *testing.T) {
	doc := map[string]any{"region": "us"}
	_, err := resolveNamespace(config.MustParseNamespace("{{region}}_products_{{tier}}"), doc)
	if err == nil {
		t.Fatal("expected error when one of multiple namespace fields is absent, got nil")
	}
	if !strings.Contains(err.Error(), "absent or null") {
		t.Errorf("error %q should mention absent or null", err.Error())
	}
}

// decodedDoc round-trips d through BSON so its values have the Go types the
// driver decodes change events and scanned documents into.
func decodedDoc(t *testing.T, d bson.D) map[string]any {
	t.Helper()
	raw, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}
	var m map[string]any
	if err := bson.Unmarshal(raw, &m); err != nil {
		t.Fatalf("bson.Unmarshal: %v", err)
	}
	return m
}

func mustDecimal128(t *testing.T, s string) bson.Decimal128 {
	t.Helper()
	d, err := bson.ParseDecimal128(s)
	if err != nil {
		t.Fatalf("bson.ParseDecimal128(%q): %v", s, err)
	}
	return d
}

func TestResolveNamespace_FormatsValue(t *testing.T) {
	// Each supported type has one printed form. A value printed with its Go
	// formatting instead, such as ObjectID("…"), is not a valid turbopuffer
	// namespace, so every write for that document is rejected and DLQ'd.
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"string", "acme-co.v2_eu", "orders_acme-co.v2_eu"},
		{"int32", int32(7), "orders_7"},
		{"int64", int64(1) << 40, "orders_1099511627776"},
		// %v prints this float64 as 1e+06.
		{"whole_double", 1e6, "orders_1000000"},
		{"negative_whole_double", -3.0, "orders_-3"},
		{"object_id", mustObjectID("507f1f77bcf86cd799439011"), "orders_507f1f77bcf86cd799439011"},
		{
			"uuid",
			bson.Binary{
				Subtype: bson.TypeBinaryUUID,
				Data: []byte{
					0x55, 0x0e, 0x84, 0x00,
					0xe2, 0x9b,
					0x41, 0xd4,
					0xa7, 0x16,
					0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
				},
			},
			"orders_550e8400-e29b-41d4-a716-446655440000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := decodedDoc(t, bson.D{{Key: "tenant", Value: tt.value}})
			got, err := resolveNamespace(config.MustParseNamespace("orders_{{tenant}}"), doc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestResolveNamespace_RejectsValueType(t *testing.T) {
	// None of these has one obvious printed form: a date could be epoch
	// milliseconds or RFC 3339, a fractional double depends on precision, and
	// a document on field order. A namespace must not hinge on that choice.
	tests := []struct {
		name  string
		value any
	}{
		{"fractional_double", 1.5},
		{"bool", true},
		{"date", time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC)},
		{"embedded_document", bson.D{{Key: "region", Value: "us"}}},
		{"array", bson.A{"a", "b"}},
		// Whole, so that only its type rejects it.
		{"decimal128", mustDecimal128(t, "7")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := decodedDoc(t, bson.D{{Key: "tenant", Value: tt.value}})
			got, err := resolveNamespace(config.MustParseNamespace("orders_{{tenant}}"), doc)
			if err == nil {
				t.Fatalf("got namespace %q, want an error for a %T value", got, doc["tenant"])
			}
		})
	}
}

func TestResolveNamespace_RejectsInvalidName(t *testing.T) {
	// Turbopuffer accepts only [A-Za-z0-9-_.]{1,128}. A resolved name outside
	// that is rejected on every write, so it must fail at transform time and
	// go to the DLQ instead.
	tests := []struct {
		name     string
		template string
		value    string
	}{
		{"space", "orders_{{tenant}}", "acme corp"},
		{"slash", "orders_{{tenant}}", "acme/eu"},
		{"colon", "orders_{{tenant}}", "acme:eu"},
		{"at_sign", "orders_{{tenant}}", "ops@acme"},
		{"quote", "orders_{{tenant}}", `acme"eu`},
		{"non_ascii", "orders_{{tenant}}", "café"},
		{"empty", "{{tenant}}", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := decodedDoc(t, bson.D{{Key: "tenant", Value: tt.value}})
			got, err := resolveNamespace(config.MustParseNamespace(tt.template), doc)
			if err == nil {
				t.Fatalf("got namespace %q, want an error", got)
			}
		})
	}
}

func TestResolveNamespace_MaxLength(t *testing.T) {
	// The limit applies to the resolved name, prefix included, not to the
	// field value alone.
	const prefix = "orders_"
	tests := []struct {
		name    string
		length  int
		wantErr bool
	}{
		{"at_limit", 128, false},
		{"over_limit", 129, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := strings.Repeat("a", tt.length-len(prefix))
			doc := decodedDoc(t, bson.D{{Key: "tenant", Value: value}})
			got, err := resolveNamespace(config.MustParseNamespace(prefix+"{{tenant}}"), doc)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("got %d-character namespace, want an error", len(got))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for a %d-character namespace: %v", tt.length, err)
			}
			if want := prefix + value; got != want {
				t.Errorf("got %q, want %q", got, want)
			}
		})
	}
}
