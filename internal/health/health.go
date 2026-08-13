package health

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// CollectionStatus tracks the health of a single collection's pipeline.
type CollectionStatus struct {
	Name          string    `json:"name"`
	LastFlushTime time.Time `json:"lastFlushTime"`
}

// Status holds health state, updated by collection goroutines.
type Status struct {
	mu          sync.RWMutex
	collections map[string]CollectionStatus
}

// NewStatus creates a Status instance.
func NewStatus() *Status {
	return &Status{
		collections: make(map[string]CollectionStatus),
	}
}

// SetCollectionFlushTime updates the last flush time for a collection.
func (s *Status) SetCollectionFlushTime(name string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.collections[name] = CollectionStatus{Name: name, LastFlushTime: t}
}

// Register adds the /healthz handler to the given mux.
func (s *Status) Register(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", s.handleHealthz)
}

func (s *Status) handleHealthz(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	colls := make([]CollectionStatus, 0, len(s.collections))
	for _, c := range s.collections {
		colls = append(colls, c)
	}

	resp := struct {
		Status      string             `json:"status"`
		Collections []CollectionStatus `json:"collections"`
	}{
		Status:      "ok",
		Collections: colls,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("healthz response write failed", "error", err)
	}
}
