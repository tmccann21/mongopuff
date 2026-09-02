package cmd

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/tmccann21/mongopuff/internal/config"
	"gopkg.in/yaml.v3"
)

var scanner = bufio.NewScanner(os.Stdin)

func prompt(msg string) string {
	fmt.Print(msg)
	scanner.Scan()
	return strings.TrimSpace(scanner.Text())
}

func promptDefault(msg, def string) string {
	fmt.Printf("%s [%s]: ", msg, def)
	scanner.Scan()
	val := strings.TrimSpace(scanner.Text())
	if val == "" {
		return def
	}
	return val
}

func promptYesNo(msg string, def bool) bool {
	suffix := " [Y/n]: "
	if !def {
		suffix = " [y/N]: "
	}
	for {
		fmt.Print(msg + suffix)
		scanner.Scan()
		val := strings.ToLower(strings.TrimSpace(scanner.Text()))
		switch val {
		case "":
			return def
		case "y", "yes":
			return true
		case "n", "no":
			return false
		default:
			fmt.Println("Please enter y or n.")
		}
	}
}

type embedPrompt struct {
	model      string
	dimensions int
	attribute  string
}

type fieldPrompt struct {
	name      string
	fieldType string
	dimension int
	precision string
	embed     *embedPrompt
}

type collectionPrompt struct {
	name          string
	namespace     string
	mirrorDeletes bool
	fields        []fieldPrompt
}

var validTypes = map[string]bool{
	"string": true, "int": true, "uint": true, "float": true,
	"bool": true, "uuid": true, "datetime": true,
	"[]string": true, "[]int": true, "[]uint": true, "[]float": true,
	"[]bool": true, "[]uuid": true, "[]datetime": true,
	"vector": true,
}

func promptField() (fieldPrompt, error) {
	name := prompt("  Field name: ")
	if name == "" {
		return fieldPrompt{}, fmt.Errorf("field name is required")
	}

	var fieldType string
	for {
		fieldType = prompt("  Field type (string, int, uint, float, bool, uuid, datetime, vector, or []<type>): ")
		if validTypes[fieldType] {
			break
		}
		fmt.Printf("  Invalid type %q. Valid types: string, int, uint, float, bool, uuid, datetime, vector, []string, []int, []uint, []float, []bool, []uuid, []datetime\n", fieldType)
	}

	f := fieldPrompt{name: name, fieldType: fieldType}

	if fieldType == "vector" {
		for {
			dimStr := prompt("  Vector dimension: ")
			dim, err := strconv.Atoi(dimStr)
			if err == nil && dim > 0 {
				f.dimension = dim
				break
			}
			fmt.Println("  Dimension must be a positive integer.")
		}

		for {
			precision := prompt("  Vector precision (f32, f16, i8): ")
			if precision == string(config.VectorPrecisionF32) ||
				precision == string(config.VectorPrecisionF16) ||
				precision == string(config.VectorPrecisionI8) {
				f.precision = precision
				break
			}
			fmt.Println("  Invalid precision. Valid options: f32, f16, i8")
		}
	}

	if fieldType == "string" && promptYesNo("  Enable embedding?", false) {
		model := prompt("  Embedding model (e.g. voyage/voyage-4): ")
		var dims int
		for {
			s := prompt("  Embedding dimensions: ")
			d, err := strconv.Atoi(s)
			if err == nil && d > 0 {
				dims = d
				break
			}
			fmt.Println("  Dimensions must be a positive integer.")
		}
		attribute := promptDefault("  Vector attribute name", name+"_vector")
		f.embed = &embedPrompt{model: model, dimensions: dims, attribute: attribute}
	}

	return f, nil
}

func promptCollection() (collectionPrompt, error) {
	name := prompt("Collection name: ")
	if name == "" {
		return collectionPrompt{}, fmt.Errorf("collection name is required")
	}

	namespace := promptDefault("Turbopuffer namespace", name)
	mirrorDeletes := promptYesNo("Mirror deletes?", true)

	fmt.Println("Add fields:")
	var fields []fieldPrompt
	for {
		f, err := promptField()
		if err != nil {
			return collectionPrompt{}, err
		}
		fields = append(fields, f)

		if !promptYesNo("Add another field?", true) {
			break
		}
	}

	return collectionPrompt{
		name:          name,
		namespace:     namespace,
		mirrorDeletes: mirrorDeletes,
		fields:        fields,
	}, nil
}

func runInit() error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	output := fs.String("o", "mongopuff.yaml", "output file path")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}

	fmt.Println("mongopuff init")
	fmt.Println()

	var collections []collectionPrompt

	for {
		coll, err := promptCollection()
		if err != nil {
			return err
		}
		collections = append(collections, coll)

		if !promptYesNo("Add another collection?", false) {
			break
		}
		fmt.Println()
	}

	fmt.Println()
	global := promptGlobal()

	return writeConfigFile(*output, collections, global)
}

func promptGlobal() config.GlobalConfig {
	flushCount := config.DefaultBatchFlushCount
	flushTimeMs := config.DefaultBatchFlushTimeMs

	if promptYesNo("Configure global batch settings?", false) {
		for {
			s := promptDefault("Batch flush count", strconv.Itoa(config.DefaultBatchFlushCount))
			n, err := strconv.Atoi(s)
			if err == nil && n > 0 {
				flushCount = n
				break
			}
			fmt.Println("Must be a positive integer.")
		}
		for {
			s := promptDefault("Batch flush interval (ms)", strconv.Itoa(config.DefaultBatchFlushTimeMs))
			n, err := strconv.Atoi(s)
			if err == nil && n > 0 {
				flushTimeMs = n
				break
			}
			fmt.Println("Must be a positive integer.")
		}
	}

	return config.GlobalConfig{
		BatchFlushCount:  flushCount,
		BatchFlushTimeMs: flushTimeMs,
	}
}

func writeConfigFile(path string, collections []collectionPrompt, global config.GlobalConfig) error {
	cfg := config.ConfigFile{
		Global:      global,
		Collections: make([]config.CollectionConfig, len(collections)),
	}

	for i, c := range collections {
		fields := make([]config.FieldMapping, len(c.fields))
		for j, f := range c.fields {
			fm := config.FieldMapping{
				Name:      f.name,
				Type:      config.FieldType(f.fieldType),
				Dimension: f.dimension,
				Precision: config.VectorPrecision(f.precision),
			}
			if f.embed != nil {
				fm.Embed = &config.Embed{
					Model:      f.embed.model,
					Dimensions: f.embed.dimensions,
					Attribute:  f.embed.attribute,
				}
			}
			fields[j] = fm
		}

		mirrorDeletes := c.mirrorDeletes
		cfg.Collections[i] = config.CollectionConfig{
			Name:          c.name,
			MirrorDeletes: &mirrorDeletes,
			Mapping: config.MappingConfig{
				Namespace: c.namespace,
				Fields:    fields,
			},
		}
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}

	fmt.Printf("Wrote %s\n", path)
	return nil
}
