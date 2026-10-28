package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"

	mongodriver "go.mongodb.org/mongo-driver/v2/mongo"
)

func checkPreImages(ctx context.Context, store *mongo.Store, collections []config.CollectionConfig) error {
	var names []string
	for _, c := range collections {
		if transform.IsDynamic(c.Mapping.Namespace) {
			names = append(names, c.Name)
		}
	}
	if len(names) == 0 {
		return nil
	}

	specs, err := store.ListCollectionSpecifications(ctx, names)
	if err != nil {
		return fmt.Errorf("listing collections for pre-image check: %w", err)
	}
	if err := verifyPreImages(collections, specs); err != nil {
		return err
	}

	slog.Info("pre-image check passed", "collections", names)
	return nil
}

func verifyPreImages(collections []config.CollectionConfig, specs []mongodriver.CollectionSpecification) error {
	byName := make(map[string]mongodriver.CollectionSpecification, len(specs))
	for _, s := range specs {
		byName[s.Name] = s
	}

	var errs []error
	for _, c := range collections {
		if !transform.IsDynamic(c.Mapping.Namespace) {
			continue
		}

		spec, ok := byName[c.Name]
		if !ok {
			errs = append(errs, fmt.Errorf(
				"collection %q has a dynamic namespace but does not exist; create it with changeStreamPreAndPostImages enabled",
				c.Name))
			continue
		}

		if enabled, _ := spec.Options.Lookup("changeStreamPreAndPostImages", "enabled").BooleanOK(); !enabled {
			errs = append(errs, fmt.Errorf(
				"collection %q has a dynamic namespace and requires changeStreamPreAndPostImages; enable it with: db.runCommand({collMod: %q, changeStreamPreAndPostImages: {enabled: true}})",
				c.Name, c.Name))
		}
	}
	return errors.Join(errs...)
}
