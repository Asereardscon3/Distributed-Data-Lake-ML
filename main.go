
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// DataRecord represents a single entry in the data lake.
type DataRecord struct {
	ID        string    `json:"id"`
	Payload   []byte    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
	Tags      []string  `json:"tags"`
}

// DataLake is a thread-safe distributed data store abstraction.
type DataLake struct {
	mu      sync.RWMutex
	storage map[string]DataRecord
}

func NewDataLake() *DataLake {
	return &DataLake{
		storage: make(map[string]DataRecord),
	}
}

// Put adds a new record to the lake.
func (dl *DataLake) Put(ctx context.Context, record DataRecord) error {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	// Simulated persistence logic
	dl.storage[record.ID] = record
	log.Printf("[DataLake] Stored record ID: %s", record.ID)
	return nil
}

// Get retrieves a record from the lake.
func (dl *DataLake) Get(ctx context.Context, id string) (DataRecord, bool) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	record, exists := dl.storage[id]
	return record, exists
}

// API Server implementation
type Server struct {
	lake *DataLake
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// In a real app, we would decode JSON from r.Body
	id := fmt.Sprintf("rec-%d", time.Now().UnixNano())
	record := DataRecord{
		ID:        id,
		Payload:   []byte("Sample ML Feature Vector"),
		Timestamp: time.Now(),
		Tags:      []string{"training", "v1"},
	}
	
	s.lake.Put(r.Context(), record)
	fmt.Fprintf(w, "Ingested record: %s", id)
}

func main() {
	lake := NewDataLake()
	server := &Server{lake: lake}

	http.HandleFunc("/ingest", server.handleIngest)
	
	port := ":8080"
	log.Printf("Distributed Data Lake ML server starting on %s...", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal(err)
	}
}
