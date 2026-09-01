package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/tmccann21/mongopuff/internal/config"
)

func runValidate() error {
	if len(os.Args) < 3 {
		return errors.New("usage: mongopuff validate <config-file>")
	}
	path := os.Args[2]

	collections, global, err := config.LoadConfigFile(path)
	if err != nil {
		return fmt.Errorf("loading config file: %w", err)
	}

	cfg := &config.AppConfig{
		Collections: collections,
		Global:      global.Effective(),
	}

	if err := config.Validate(cfg); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	fmt.Println("config is valid")
	return nil
}
