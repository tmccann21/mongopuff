package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/tmccann21/mongopuff/internal/config"

	mongodriver "go.mongodb.org/mongo-driver/v2/mongo"
)

// collectionLister is the part of *mongo.Store that checkPreImages uses.
type collectionLister interface {
	ListCollectionSpecifications(ctx context.Context, names []string) ([]mongodriver.CollectionSpecification, error)
}

func checkPreImages(ctx context.Context, lister collectionLister, collections []config.CollectionConfig) error {
	var names []string
	for _, c := range collections {
		if c.Mapping.Namespace.IsDynamic() {
			names = append(names, c.Name)
		}
	}
	if len(names) == 0 {
		return nil
	}

	specs, err := lister.ListCollectionSpecifications(ctx, names)
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
		if !c.Mapping.Namespace.IsDynamic() {
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
