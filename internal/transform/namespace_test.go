package transform

import (
	"strings"
	"testing"
)

func TestResolveNamespace_SingleTokenPresent(t *testing.T) {
	doc := map[string]any{"tenant": "acme"}
	got, err := ResolveNamespace("products_{{tenant}}", doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "products_acme" {
		t.Errorf("got %q, want %q", got, "products_acme")
	}
}

func TestResolveNamespace_SingleTokenAbsent(t *testing.T) {
	doc := map[string]any{"other": "value"}
	_, err := ResolveNamespace("products_{{tenant}}", doc)
	if err == nil {
		t.Fatal("expected error for absent namespace field, got nil")
	}
	if !strings.Contains(err.Error(), "absent or null") {
		t.Errorf("error %q should mention absent or null", err.Error())
	}
}

func TestResolveNamespace_SingleTokenNull(t *testing.T) {
	doc := map[string]any{"tenant": nil}
	_, err := ResolveNamespace("products_{{tenant}}", doc)
	if err == nil {
		t.Fatal("expected error for null namespace field, got nil")
	}
	if !strings.Contains(err.Error(), "absent or null") {
		t.Errorf("error %q should mention absent or null", err.Error())
	}
}

func TestResolveNamespace_MultiTokenAllPresent(t *testing.T) {
	doc := map[string]any{"region": "us", "tier": "gold"}
	got, err := ResolveNamespace("{{region}}_products_{{tier}}", doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "us_products_gold" {
		t.Errorf("got %q, want %q", got, "us_products_gold")
	}
}

func TestResolveNamespace_MultiTokenOneAbsent(t *testing.T) {
	doc := map[string]any{"region": "us"}
	_, err := ResolveNamespace("{{region}}_products_{{tier}}", doc)
	if err == nil {
		t.Fatal("expected error when one of multiple namespace fields is absent, got nil")
	}
	if !strings.Contains(err.Error(), "absent or null") {
		t.Errorf("error %q should mention absent or null", err.Error())
	}
}

func TestResolveNamespace_NestedPathRejected(t *testing.T) {
	doc := map[string]any{}
	_, err := ResolveNamespace("products_{{meta.tenant}}", doc)
	if err == nil {
		t.Fatal("expected error for nested field path, got nil")
	}
	if !strings.Contains(err.Error(), "nested") {
		t.Errorf("error %q should mention nested", err.Error())
	}
}
