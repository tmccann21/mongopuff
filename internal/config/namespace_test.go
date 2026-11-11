package config

import (
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestParseNamespace_RejectsInvalid(t *testing.T) {
	// A namespace that can never resolve to a valid turbopuffer name must fail
	// once at startup, not once per event in the DLQ.
	tests := []struct {
		name      string
		namespace string
	}{
		{"empty_token", "orders_{{}}"},
		{"unclosed_token", "orders_{{tenant"},
		{"unopened_token", "orders_tenant}}"},
		{"nested_path", "orders_{{meta.tenant}}"},
		{"static_invalid_char", "my orders"},
		{"static_too_long", strings.Repeat("a", 129)},
		// The literal text around a token is part of every resolved name.
		{"dynamic_invalid_literal", "my orders_{{tenant}}"},
		{"dynamic_literal_too_long", strings.Repeat("a", 129) + "{{tenant}}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, err := ParseNamespace(tt.namespace)
			if err == nil {
				t.Fatalf("got %+v, want an error for namespace %q", ns, tt.namespace)
			}
			if !strings.Contains(err.Error(), strconv.Quote(tt.namespace)) {
				t.Errorf("error %q should quote the namespace", err.Error())
			}
		})
	}
}

func TestParseNamespace_NestedPathRejected(t *testing.T) {
	_, err := ParseNamespace("products_{{meta.tenant}}")
	if err == nil {
		t.Fatal("expected error for nested field path, got nil")
	}
	if !strings.Contains(err.Error(), "nested") {
		t.Errorf("error %q should mention nested", err.Error())
	}
}

func TestParseNamespace_Accepts(t *testing.T) {
	// Guards the rules above against rejecting a valid static name or template.
	for _, s := range []string{
		"orders-v2.prod_eu",
		strings.Repeat("a", 128),
		"{{tenant}}",
		"orders_{{tenant}}",
		"{{region}}_orders_{{tier}}",
		"{{region}}{{tier}}",
	} {
		ns, err := ParseNamespace(s)
		if err != nil {
			t.Errorf("ParseNamespace(%q): unexpected error: %v", s, err)
			continue
		}
		if got := ns.String(); got != s {
			t.Errorf("ParseNamespace(%q).String() = %q", s, got)
		}
	}
}

// IsDynamic gates the pre-image check, fullDocumentBeforeChange on the change
// stream, and namespace transition detection. A static namespace it reports as
// dynamic turns on all three.
func TestNamespace_IsDynamic(t *testing.T) {
	tests := []struct {
		namespace string
		want      bool
	}{
		{"users", false},
		{"order_data", false},
		{"orders-v2.prod", false},
		{"orders_{{tenant}}", true},
		{"{{region}}_orders_{{tier}}", true},
	}
	for _, tt := range tests {
		if got := MustParseNamespace(tt.namespace).IsDynamic(); got != tt.want {
			t.Errorf("IsDynamic(%q) = %v, want %v", tt.namespace, got, tt.want)
		}
	}
}

func TestNamespace_YAML(t *testing.T) {
	// The config file is parsed on load, so a bad template fails there.
	t.Run("rejects_invalid", func(t *testing.T) {
		var m MappingConfig
		if err := yaml.Unmarshal([]byte("namespace: orders_{{}}\n"), &m); err == nil {
			t.Fatalf("got %+v, want an error", m)
		}
	})

	// init writes the config with yaml.Marshal, and run must load it back.
	t.Run("round_trip", func(t *testing.T) {
		for _, s := range []string{"orders", "{{tenant}}", "orders_{{tenant}}"} {
			data, err := yaml.Marshal(MappingConfig{Namespace: MustParseNamespace(s)})
			if err != nil {
				t.Fatalf("yaml.Marshal(%q): %v", s, err)
			}
			var m MappingConfig
			if err := yaml.Unmarshal(data, &m); err != nil {
				t.Fatalf("yaml.Unmarshal(%q): %v", data, err)
			}
			if got := m.Namespace.String(); got != s {
				t.Errorf("round trip of %q = %q", s, got)
			}
			if got, want := m.Namespace.IsDynamic(), MustParseNamespace(s).IsDynamic(); got != want {
				t.Errorf("round trip of %q: IsDynamic() = %v, want %v", s, got, want)
			}
		}
	})
}
