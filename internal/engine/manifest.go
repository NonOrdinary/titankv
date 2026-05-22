package engine

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type ManifestRecord struct {
	Action string `json:"action"`
	Path   string `json:"path"`
	MinKey string `json:"min_key"`
	MaxKey string `json:"max_key"`
}

type Manifest struct {
	file *os.File
	mu   sync.Mutex
	path string
}

func NewManifest(path string) (*Manifest, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Manifest{
		file: f,
		path: path,
	}, nil
}

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

	return m.file.Sync()
}

func (m *Manifest) Compact(activeRecords []ManifestRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tempPath := m.path + ".tmp"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	for _, rec := range activeRecords {
		rec.Action = "ADD"
		data, _ := json.Marshal(rec)
		data = append(data, '\n')
		tempFile.Write(data)
	}

	tempFile.Sync()
	tempFile.Close()

	m.file.Close()

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

func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.file.Close()
}

func RecoverManifest(path string) ([]ManifestRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []ManifestRecord{}, nil
		}
		return nil, err
	}
	defer f.Close()

	var records []ManifestRecord

	isDead := make(map[int]bool)
	pathToIdx := make(map[string]int)

	scanner := bufio.NewScanner(f)
	idx := 0

	for scanner.Scan() {
		var rec ManifestRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			break
		}

		if rec.Action == "ADD" {
			records = append(records, rec)
			pathToIdx[rec.Path] = idx
			idx++
		} else if rec.Action == "REMOVE" {
			if targetIdx, exists := pathToIdx[rec.Path]; exists {
				isDead[targetIdx] = true
			}
		}
	}

	var activeTables []ManifestRecord
	for i, rec := range records {
		if !isDead[i] {
			activeTables = append(activeTables, rec)
		}
	}

	return activeTables, nil
}
