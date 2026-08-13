package cmd

import (
	"context"
	"fmt"

	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/mongo"
)

func loadConfig(ctx context.Context) (*config.AppConfig, *mongo.Store, error) {
	cfg, err := config.LoadEnv()
	if err != nil {
		return nil, nil, fmt.Errorf("loading environment variables: %w", err)
	}

	store, err := mongo.NewStore(ctx, cfg.MongoDBConnectionString)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to mongodb: %w", err)
	}

	collections, err := store.LoadCollectionConfigs(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("loading collection configs: %w", err)
	}
	cfg.Collections = collections

	global, err := store.LoadGlobalConfig(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("loading global config: %w", err)
	}
	cfg.Global = global.Effective()

	if err := config.Validate(cfg); err != nil {
		return nil, nil, fmt.Errorf("validating config: %w", err)
	}

	return cfg, store, nil
}
