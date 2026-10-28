package cmd

import (
	"github.com/tmccann21/mongopuff/internal/config"

	mongodriver "go.mongodb.org/mongo-driver/v2/mongo"
)

// verifyPreImages checks that every collection with a dynamic namespace has
// changeStreamPreAndPostImages enabled.
//
// TODO: not yet implemented; preimages_test.go defines the expected behavior.
func verifyPreImages(collections []config.CollectionConfig, specs []mongodriver.CollectionSpecification) error {
	return nil
}
