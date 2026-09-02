package cmd

import (
	"errors"
	"fmt"
	"os"
)

func Execute() error {
	if len(os.Args) < 2 {
		return errors.New("usage: mongopuff <run|backfill|dlq|validate|init>")
	}

	switch os.Args[1] {
	case "run":
		return runCDC()
	case "backfill":
		return runBackfill()
	case "dlq":
		return runDLQ()
	case "validate":
		return runValidate()
	case "init":
		return runInit()
	default:
		return fmt.Errorf("unknown command: %s", os.Args[1])
	}
}
