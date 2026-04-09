package engine

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// ManifestRecord represents a single change to the database's file state.
type ManifestRecord struct {
	Action string `json:"action"` // "ADD" or "REMOVE"
	Path   string `json:"path"`
	MinKey string `json:"min_key"`
	MaxKey string `json:"max_key"`
}

// Manifest manages the durable log of active SSTables.
type Manifest struct {
	file *os.File
	mu   sync.Mutex
	path string // Stored so we know where to save during Compaction
}

// NewManifest opens or creates the append-only manifest file.
func NewManifest(path string) (*Manifest, error) {
	// FIX 2: O_SYNC removed to stop hardware bottlenecking.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Manifest{
		file: f,
		path: path,
	}, nil
}

// Append writes a new state change to the log and explicitly syncs it.
func (m *Manifest) Append(record ManifestRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	data = append(data, '\n')
	if _, err := m.file.Write(data); err != nil {
		return err
	}

	// FIX 2: Manual fsync guarantees durability without the O_SYNC penalty
	return m.file.Sync()
}

// Compact shrinks the manifest file to prevent infinite growth.
// FIX 4: Write to a temp file, then atomic rename.
func (m *Manifest) Compact(activeRecords []ManifestRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tempPath := m.path + ".tmp"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	for _, rec := range activeRecords {
		// Force action to ADD because this is the new baseline state
		rec.Action = "ADD"
		data, _ := json.Marshal(rec)
		data = append(data, '\n')
		tempFile.Write(data)
	}

	tempFile.Sync()
	tempFile.Close()

	// Re-open our file descriptor to point to the newly swapped file
	m.file.Close()
	// POSIX Atomic Rename: If power fails here, the old manifest is completely unharmed.
	if err := os.Rename(tempPath, m.path); err != nil {
		return err
	}

	f, err := os.OpenFile(m.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	m.file = f

	return nil
}

// Close safely shuts down the manifest file.
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.file.Close()
}

// RecoverManifest rebuilds the chronological array of active SSTables.
func RecoverManifest(path string) ([]ManifestRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []ManifestRecord{}, nil // Fresh DB
		}
		return nil, err
	}
	defer f.Close()

	var records []ManifestRecord

	// FIX 3: O(1) Deletion Tracking
	isDead := make(map[int]bool)
	pathToIdx := make(map[string]int)

	scanner := bufio.NewScanner(f)
	idx := 0

	for scanner.Scan() {
		var rec ManifestRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			// FIX 1: Graceful Degradation on Torn Write
			// We hit half-written garbage. Stop reading and boot with what we have.
			break
		}

		if rec.Action == "ADD" {
			records = append(records, rec)
			pathToIdx[rec.Path] = idx
			idx++
		} else if rec.Action == "REMOVE" {
			if targetIdx, exists := pathToIdx[rec.Path]; exists {
				isDead[targetIdx] = true // Mark tombstone in O(1) time
			}
		}
	}

	// FIX 3: Single O(N) pass to rebuild the final array, perfectly preserving chronological order
	var activeTables []ManifestRecord
	for i, rec := range records {
		if !isDead[i] {
			activeTables = append(activeTables, rec)
		}
	}

	return activeTables, nil
}
