package turbopuffer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/tmccann21/mongopuff/internal/config"
	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"
	"github.com/turbopuffer/turbopuffer-go/v2"
	"github.com/turbopuffer/turbopuffer-go/v2/option"
	"github.com/turbopuffer/turbopuffer-go/v2/packages/param"
)

type Client struct {
	client turbopuffer.Client
}

func New(apiKey string, region string) *Client {
	return &Client{
		client: turbopuffer.NewClient(
			option.WithAPIKey(apiKey),
			option.WithRegion(region),
		),
	}
}

func BuildSchema(fields []config.FieldMapping) map[string]turbopuffer.AttributeSchemaConfigParam {
	schema := make(map[string]turbopuffer.AttributeSchemaConfigParam, len(fields))
	for _, field := range fields {
		entry := turbopuffer.AttributeSchemaConfigParam{
			Type: turbopuffer.AttributeType(field.Type),
		}
		if field.Filterable != nil {
			entry.Filterable = param.NewOpt(*field.Filterable)
		}
		schema[field.Name] = entry
	}
	return schema
}

func toRow(a transform.Action) turbopuffer.RowParam {
	row := turbopuffer.RowParam{
		"id":           a.DocumentID,
		"_clusterTime": int64(a.ClusterTime),
	}
	for k, v := range a.Attributes {
		row[k] = v
	}

	return row
}

func (c *Client) Write(ctx context.Context, namespace string, schema map[string]turbopuffer.AttributeSchemaConfigParam, actions []transform.Action) error {
	ns := c.client.Namespace(namespace)

	var upsertRows []turbopuffer.RowParam
	var patchRows []turbopuffer.RowParam
	var deletes []any

	for _, action := range actions {
		switch action.Type {
		case transform.ActionUpsert:
			upsertRows = append(upsertRows, toRow(action))
		case transform.ActionPatch:
			patchRows = append(patchRows, toRow(action))
		case transform.ActionDelete:
			deletes = append(deletes, action.DocumentID)
		}
	}

	params := turbopuffer.NamespaceWriteParams{
		Schema: schema,
	}

	if len(upsertRows) > 0 {
		params.UpsertRows = upsertRows
		params.UpsertCondition = turbopuffer.NewFilterLt(
			"_clusterTime",
			map[string]string{"$ref_new": "_clusterTime"},
		)
	}

	if len(patchRows) > 0 {
		params.PatchRows = patchRows
		params.PatchCondition = turbopuffer.NewFilterLt(
			"_clusterTime",
			map[string]string{"$ref_new": "_clusterTime"},
		)
	}

	if len(deletes) > 0 {
		params.Deletes = deletes
	}

	_, err := ns.Write(ctx, params)

	if err != nil {
		return fmt.Errorf("failed to write to turbopuffer: %w", err)
	}

	slog.Info("turbopuffer write success",
		"namespace", namespace,
		"upserts", len(upsertRows),
		"patches", len(patchRows),
		"deletes", len(deletes),
	)
	return nil
}

func ClassifyError(err error) mongo.ErrorKind {
	var apierr *turbopuffer.Error
	if errors.As(err, &apierr) {
		switch {
		case apierr.StatusCode == 429:
			return mongo.ErrRateLimited
		case apierr.StatusCode >= 500:
			return mongo.ErrServerError
		default:
			return mongo.ErrWriteRejected
		}
	}
	return mongo.ErrNetworkError
}
