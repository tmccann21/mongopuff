package mongo

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// resolveChangeStreamOptions applies the builder's setters to an empty
// ChangeStreamOptions, as the driver does before it sends the aggregate.
func resolveChangeStreamOptions(t *testing.T, b *options.ChangeStreamOptionsBuilder) options.ChangeStreamOptions {
	t.Helper()
	var opts options.ChangeStreamOptions
	for _, set := range b.List() {
		if err := set(&opts); err != nil {
			t.Fatalf("applying change stream option: %v", err)
		}
	}
	return opts
}

// fullDocumentValue renders an optional FullDocument for failure messages,
// since %+v on ChangeStreamOptions prints only its pointer addresses.
func fullDocumentValue(fd *options.FullDocument) string {
	if fd == nil {
		return "unset"
	}
	return string(*fd)
}

func TestChangeStreamOptions(t *testing.T) {
	updateLookup := options.UpdateLookup
	whenAvailable := options.WhenAvailable

	token, err := bson.Marshal(bson.D{{Key: "_data", Value: "8267A1B2C3000000012B0429296E1404"}})
	if err != nil {
		t.Fatalf("bson.Marshal: %v", err)
	}

	// run.go passes preImages = transform.IsDynamic(namespace). The whole struct
	// is compared, so a static stream that gains any option fails, not only one
	// that gains fullDocumentBeforeChange.
	tests := []struct {
		name        string
		resumeToken []byte
		preImages   bool
		want        options.ChangeStreamOptions
	}{
		{
			name: "static_namespace",
			want: options.ChangeStreamOptions{FullDocument: &updateLookup},
		},
		{
			name:        "static_namespace_resumed",
			resumeToken: token,
			want: options.ChangeStreamOptions{
				FullDocument: &updateLookup,
				ResumeAfter:  bson.Raw(token),
			},
		},
		{
			name:      "dynamic_namespace",
			preImages: true,
			want: options.ChangeStreamOptions{
				FullDocument:             &updateLookup,
				FullDocumentBeforeChange: &whenAvailable,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveChangeStreamOptions(t, changeStreamOptions(tt.resumeToken, tt.preImages))
			if g, w := fullDocumentValue(got.FullDocument), fullDocumentValue(tt.want.FullDocument); g != w {
				t.Errorf("fullDocument = %s, want %s", g, w)
			}
			if g, w := fullDocumentValue(got.FullDocumentBeforeChange), fullDocumentValue(tt.want.FullDocumentBeforeChange); g != w {
				t.Errorf("fullDocumentBeforeChange = %s, want %s", g, w)
			}
			if !t.Failed() && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("change stream options = %+v, want %+v", got, tt.want)
			}
		})
	}
}
