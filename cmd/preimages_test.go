package cmd

import (
	"context"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tmccann21/mongopuff/internal/config"

	mongodriver "go.mongodb.org/mongo-driver/v2/mongo"
)

// Collection options below mirror listCollections output captured from
// MongoDB 8.2. Enabling pre-images stores the option on the collection;
// disabling it via collMod removes the key entirely rather than storing
// enabled: false, so disabled and never-configured collections both
// report empty options.
var (
	preImagesEnabled  = bson.D{{Key: "changeStreamPreAndPostImages", Value: bson.D{{Key: "enabled", Value: true}}}}
	preImagesDisabled = bson.D{}
)

func mustCollSpec(name string, opts bson.D) mongodriver.CollectionSpecification {
	raw, err := bson.Marshal(opts)
	if err != nil {
		panic(err)
	}
	return mongodriver.CollectionSpecification{Name: name, Type: "collection", Options: raw}
}

func collConfig(name, namespace string) config.CollectionConfig {
	return config.CollectionConfig{Name: name, Mapping: config.MappingConfig{Namespace: config.MustParseNamespace(namespace)}}
}

func TestVerifyPreImages(t *testing.T) {
	tests := []struct {
		name        string
		collections []config.CollectionConfig
		specs       []mongodriver.CollectionSpecification
		wantErr     []string // substrings the error must contain; nil means no error expected
	}{
		{
			name:        "dynamic_enabled",
			collections: []config.CollectionConfig{collConfig("orders", "orders_{{tenant}}")},
			specs:       []mongodriver.CollectionSpecification{mustCollSpec("orders", preImagesEnabled)},
		},
		{
			// The key is absent, so Lookup returns an empty RawValue. Reading
			// it with Boolean() instead of BooleanOK() panics.
			name:        "dynamic_disabled",
			collections: []config.CollectionConfig{collConfig("orders", "orders_{{tenant}}")},
			specs:       []mongodriver.CollectionSpecification{mustCollSpec("orders", preImagesDisabled)},
			wantErr:     []string{`"orders"`, "changeStreamPreAndPostImages"},
		},
		{
			name:        "dynamic_collection_missing",
			collections: []config.CollectionConfig{collConfig("orders", "orders_{{tenant}}")},
			specs:       nil,
			wantErr:     []string{`"orders"`, "does not exist"},
		},
		{
			// Existing deployments never enabled pre-images. Checking static
			// collections would stop every one of them from starting.
			name:        "static_disabled_skipped",
			collections: []config.CollectionConfig{collConfig("users", "users")},
			specs:       []mongodriver.CollectionSpecification{mustCollSpec("users", preImagesDisabled)},
		},
		{
			// A change stream can watch a collection that does not exist yet.
			// The "does not exist" error is for dynamic namespaces only.
			name:        "static_collection_missing_skipped",
			collections: []config.CollectionConfig{collConfig("users", "users")},
			specs:       nil,
		},
		{
			name: "multiple_failures_reported",
			collections: []config.CollectionConfig{
				collConfig("orders", "orders_{{tenant}}"),
				collConfig("users", "users"),
				collConfig("invoices", "invoices_{{tenant}}"),
			},
			specs: []mongodriver.CollectionSpecification{
				mustCollSpec("orders", preImagesDisabled),
				mustCollSpec("users", preImagesDisabled),
				mustCollSpec("invoices", preImagesDisabled),
			},
			wantErr: []string{`"orders"`, `"invoices"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verifyPreImages(tt.collections, tt.specs)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			for _, want := range tt.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q should contain %q", err.Error(), want)
				}
			}
		})
	}
}

func TestCheckPreImages_StaticNamespacesSkipListCollections(t *testing.T) {
	// listCollections needs a privilege that a static deployment's user may
	// not have been granted, so querying it for static collections could stop
	// an existing deployment from starting. The store is nil: any query
	// panics, and the recover turns that into a test failure.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("checkPreImages queried MongoDB for static collections: %v", r)
		}
	}()

	collections := []config.CollectionConfig{collConfig("users", "users"), collConfig("orders", "order_data")}
	if err := checkPreImages(context.Background(), nil, collections); err != nil {
		t.Fatalf("checkPreImages: %v", err)
	}
}
