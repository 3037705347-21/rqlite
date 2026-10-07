package backup

import (
	"context"
	"expvar"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/auto/file"
)

func Test_Uploader_FileStorage_Timestamped(t *testing.T) {
	ResetStats()

	dir := t.TempDir()
	name := "backup.sqlite"
	storageClient, err := file.NewClient(dir, name, &file.Options{Timestamp: true})
	if err != nil {
		t.Fatalf("failed to create file storage client: %s", err.Error())
	}
	dp := &mockDataProvider{data: "test backup data"}
	interval := 10 * time.Millisecond
	uploader := NewUploader(storageClient, dp, interval)

	// Track last index values to force multiple uploads
	lastIndexValue := atomic.Uint64{}
	lastIndexValue.Store(1)
	dp.lastIndexFn = func() (uint64, error) {
		return lastIndexValue.Load(), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	uploader.Start(ctx, nil)
	defer cancel()

	// Wait for first upload
	testPoll(t, func() bool {
		md, err := storageClient.CurrentMetadata(context.Background())
		return err == nil && md != nil
	}, 10*time.Millisecond, time.Second)

	// Add a small delay to ensure different timestamp
	time.Sleep(1100 * time.Millisecond)

	// Increment last index to trigger another upload
	lastIndexValue.Store(2)

	// Wait for second upload
	testPoll(t, func() bool {
		files, err := os.ReadDir(dir)
		if err != nil {
			return false
		}
		timestampedFiles := 0
		for _, file := range files {
			if strings.HasSuffix(file.Name(), "_backup.sqlite") {
				timestampedFiles++
			}
		}
		return timestampedFiles >= 2
	}, 50*time.Millisecond, 5*time.Second)

	// Verify we have at least 2 timestamped files
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to read directory: %s", err.Error())
	}
	timestampedFiles := 0
	for _, file := range files {
		if strings.HasSuffix(file.Name(), "_backup.sqlite") {
			timestampedFiles++
		}
	}
	if timestampedFiles < 2 {
		t.Fatalf("expected at least 2 timestamped files, got %d", timestampedFiles)
	}

	// Verify metadata points to the latest file
	md, err := storageClient.CurrentMetadata(context.Background())
	if err != nil {
		t.Fatalf("failed to get current metadata: %s", err.Error())
	}
	if md == nil {
		t.Fatal("metadata is nil")
	}

	// Verify data in latest file is correct
	dataPath := storageClient.LatestFilePath(context.Background())
	if dataPath == "" {
		t.Fatal("latest file path is empty")
	}
	data, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("failed to read data file: %s", err.Error())
	}
	if string(data) != "test backup data" {
		t.Fatalf("data mismatch: got %q want %q", string(data), "test backup data")
	}
}

func Test_Uploader_FileStorage(t *testing.T) {
	ResetStats()

	dir := t.TempDir()
	name := "data"
	storageClient, err := file.NewClient(dir, name, nil)
	if err != nil {
		t.Fatalf("failed to create file storage client: %s", err.Error())
	}
	dp := &mockDataProvider{data: "my upload data"}
	interval := 10 * time.Millisecond
	uploader := NewUploader(storageClient, dp, interval)

	uploaded := false
	enabledFn := func() bool {
		if !uploaded {
			uploaded = true
			return true
		}
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	uploader.Start(ctx, enabledFn)
	defer cancel()

	testPoll(t, func() bool {
		md, err := storageClient.CurrentMetadata(context.Background())
		return err == nil && md != nil
	}, 10*time.Millisecond, time.Second)

	md, err := storageClient.CurrentMetadata(context.Background())
	if err != nil {
		t.Fatalf("failed to get current metadata: %s", err.Error())
	}
	if md == nil {
		t.Fatal("metadata is nil")
	}
	if md.ID != "1" {
		t.Fatalf("expected metadata ID to be 1, got %s", md.ID)
	}

	dataPath := storageClient.LatestFilePath(context.Background())
	data, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatalf("failed to read data file: %s", err.Error())
	}
	if string(data) != "my upload data" {
		t.Fatalf("data mismatch: got %q want %q", string(data), "my upload data")
	}
}

// Test_MultiUploader_RestartReconcile_File verifies that a freshly-started
// multi-destination uploader reconciles each file target from its
// METADATA.json: already-current targets are not re-uploaded, while a target
// that holds nothing is caught up with the current generation.
func Test_MultiUploader_RestartReconcile_File(t *testing.T) {
	ResetStats()

	dirA := t.TempDir()
	dirB := t.TempDir()
	newClients := func(aDir, bDir string) []*file.Client {
		ca, err := file.NewClient(aDir, "db.sqlite", nil)
		if err != nil {
			t.Fatalf("file client a: %s", err)
		}
		cb, err := file.NewClient(bDir, "db.sqlite", nil)
		if err != nil {
			t.Fatalf("file client b: %s", err)
		}
		return []*file.Client{ca, cb}
	}
	destinationsFor := func(clients []*file.Client) []Destination {
		dests := make([]Destination, len(clients))
		for i, c := range clients {
			dests[i] = Destination{Name: fmt.Sprintf("dir-%c", 'a'+rune(i)), Client: c}
		}
		return dests
	}

	dp := &mockDataProvider{data: "shared backup bytes"}

	// First run: both destinations receive generation 1.
	clients := newClients(dirA, dirB)
	u, err := NewUploaderMulti(destinationsFor(clients), dp, time.Second)
	if err != nil {
		t.Fatalf("NewUploaderMulti: %s", err)
	}
	if err := u.uploadMulti(context.Background()); err != nil {
		t.Fatalf("first generation: %s", err)
	}
	for i, c := range clients {
		id, err := c.CurrentID(context.Background())
		if err != nil || id != "1" {
			t.Fatalf("destination %d: expected ID 1, got %q (%v)", i, id, err)
		}
	}

	// Simulate an uploader restart against the same directories. Both
	// targets already hold generation 1, so nothing is uploaded.
	u2, err := NewUploaderMulti(destinationsFor(newClients(dirA, dirB)), dp, time.Second)
	if err != nil {
		t.Fatalf("NewUploaderMulti: %s", err)
	}
	okBefore := stats.Get(numUploadsOK).(*expvar.Int).Value()
	if err := u2.uploadMulti(context.Background()); err != nil {
		t.Fatalf("reconciliation cycle: %s", err)
	}
	if stats.Get(numUploadsOK).(*expvar.Int).Value() != okBefore {
		t.Fatalf("expected no uploads when all destinations are current")
	}
	if stats.Get(numUploadsSkippedID).(*expvar.Int).Value() < 2 {
		t.Fatalf("expected both current destinations to be skipped by ID")
	}
	st, _ := u2.Stats()
	if st["generation_complete"] != true {
		t.Fatalf("expected reconciled generation to be complete")
	}

	// Another restart, but target b's directory is empty: only b is caught
	// up; a is not re-uploaded.
	dirBEmpty := t.TempDir()
	clients3 := newClients(dirA, dirBEmpty)
	u3, err := NewUploaderMulti(destinationsFor(clients3), dp, time.Second)
	if err != nil {
		t.Fatalf("NewUploaderMulti: %s", err)
	}
	if err := u3.uploadMulti(context.Background()); err != nil {
		t.Fatalf("catch-up cycle: %s", err)
	}
	idB, err := clients3[1].CurrentID(context.Background())
	if err != nil {
		t.Fatalf("destination b current ID: %s", err)
	}
	if idB != "1" {
		t.Fatalf("expected caught-up destination b to hold generation 1, got %s", idB)
	}
	data, err := os.ReadFile(clients3[1].LatestFilePath(context.Background()))
	if err != nil {
		t.Fatalf("read caught-up backup: %s", err)
	}
	if string(data) != "shared backup bytes" {
		t.Fatalf("caught-up data mismatch: %q", string(data))
	}
	st3, _ := u3.Stats()
	destStats := st3["destinations"].([]map[string]any)
	if destStats[0]["last_uploaded_id"] != "1" || destStats[1]["last_uploaded_id"] != "1" {
		t.Fatalf("expected both destinations to be at generation 1, got %+v", destStats)
	}
	if st3["generation_complete"] != true {
		t.Fatalf("expected generation to be complete after catching destination b up")
	}
}
