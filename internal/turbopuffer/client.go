package turbopuffer

import (
	"context"
	"errors"
	"fmt"

	"github.com/tmccann21/mongopuff/internal/mongo"
	"github.com/tmccann21/mongopuff/internal/transform"
	"github.com/turbopuffer/turbopuffer-go/v2"
	"github.com/turbopuffer/turbopuffer-go/v2/option"
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

func (c *Client) Write(ctx context.Context, namespace string, actions []transform.Action) error {
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

	condition := turbopuffer.NewFilterLt(
		"_clusterTime",
		map[string]string{"$ref_new": "_clusterTime"},
	)

	_, err := ns.Write(ctx, turbopuffer.NamespaceWriteParams{
		UpsertRows:      upsertRows,
		UpsertCondition: condition,
		PatchRows:       patchRows,
		PatchCondition:  condition,
		Deletes:         deletes,
	})

	if err != nil {
		return fmt.Errorf("failed to write to turbopuffer: %w", err)
	}
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
