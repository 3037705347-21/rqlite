package backup

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockDestinationClient is a StorageClient that independently records every
// upload and CurrentID reconciliation made against it.
type mockDestinationClient struct {
	name string

	mu             sync.Mutex
	uploadIDs      []string // all attempted upload IDs, including failed ones
	uploadOK       []bool   // whether each attempt in uploadIDs succeeded
	uploadData     [][]byte
	uploadErrFn    func(call int) error
	currentID      string
	currentIDErr   error
	currentIDCalls int
}

func (m *mockDestinationClient) Upload(ctx context.Context, reader io.Reader, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploadIDs = append(m.uploadIDs, id)
	ok := true
	if m.uploadErrFn != nil {
		if err := m.uploadErrFn(len(m.uploadIDs)); err != nil {
			ok = false
			m.uploadOK = append(m.uploadOK, ok)
			return err
		}
	}
	m.uploadOK = append(m.uploadOK, ok)
	m.uploadData = append(m.uploadData, data)
	return nil
}

func (m *mockDestinationClient) CurrentID(ctx context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentIDCalls++
	return m.currentID, m.currentIDErr
}

func (m *mockDestinationClient) String() string {
	return "mock://" + m.name
}

func (m *mockDestinationClient) calls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	calls := make([]string, len(m.uploadIDs))
	copy(calls, m.uploadIDs)
	return calls
}

// successIDs returns the IDs of upload attempts that succeeded, in order.
func (m *mockDestinationClient) successIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for i, id := range m.uploadIDs {
		if m.uploadOK[i] {
			ids = append(ids, id)
		}
	}
	return ids
}

func (m *mockDestinationClient) lastData() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.uploadData) == 0 {
		return nil
	}
	return m.uploadData[len(m.uploadData)-1]
}

func newTestMultiUploader(t *testing.T, dp DataProvider, clients ...*mockDestinationClient) *Uploader {
	t.Helper()
	dests := make([]Destination, 0, len(clients))
	for _, c := range clients {
		dests = append(dests, Destination{Name: c.name, Client: c})
	}
	u, err := NewUploaderMulti(dests, dp, time.Second)
	if err != nil {
		t.Fatalf("failed to create multi-uploader: %v", err)
	}
	return u
}

func Test_NewUploaderMulti_Empty(t *testing.T) {
	dp := &mockDataProvider{data: "x"}
	if _, err := NewUploaderMulti(nil, dp, time.Second); err == nil {
		t.Fatalf("expected error for empty destination list")
	}
}

func Test_MultiUploaderAllSucceed(t *testing.T) {
	ResetStats()
	a := &mockDestinationClient{name: "a", currentIDErr: fmt.Errorf("not found")}
	b := &mockDestinationClient{name: "b", currentIDErr: fmt.Errorf("not found")}
	c := &mockDestinationClient{name: "c", currentIDErr: fmt.Errorf("not found")}
	dp := &mockDataProvider{data: "gen1"}
	u := newTestMultiUploader(t, dp, a, b, c)

	if err := u.uploadMulti(context.Background()); err != nil {
		t.Fatalf("expected generation to complete, got %v", err)
	}
	for _, m := range []*mockDestinationClient{a, b, c} {
		if calls := m.calls(); len(calls) != 1 || calls[0] != "1" {
			t.Fatalf("destination %s: expected single upload of generation 1, got %v", m.name, calls)
		}
		if got := string(m.lastData()); got != "gen1" {
			t.Fatalf("destination %s: uploaded data %q, want gen1", m.name, got)
		}
	}
	if got := stats.Get(numUploadsOK).(*expvar.Int).Value(); got != 3 {
		t.Fatalf("expected 3 successful uploads, got %d", got)
	}

	st, err := u.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st["last_generation_id"] != "1" {
		t.Fatalf("expected last_generation_id 1, got %v", st["last_generation_id"])
	}
	if st["generation_complete"] != true {
		t.Fatalf("expected generation_complete true, got %v", st["generation_complete"])
	}
	destStats := st["destinations"].([]map[string]any)
	for i, ds := range destStats {
		if ds["last_uploaded_id"] != "1" {
			t.Fatalf("destination %d: expected last_uploaded_id 1, got %v", i, ds["last_uploaded_id"])
		}
		if ds["last_error"] != "" {
			t.Fatalf("destination %d: unexpected last_error %v", i, ds["last_error"])
		}
	}
}

func Test_MultiUploaderFailedTargetOnlyIsRetried(t *testing.T) {
	ResetStats()
	a := &mockDestinationClient{name: "a", currentIDErr: fmt.Errorf("not found")}
	b := &mockDestinationClient{
		name:         "b",
		currentIDErr: fmt.Errorf("not found"),
		uploadErrFn: func(call int) error {
			if call == 1 {
				return fmt.Errorf("connection refused")
			}
			return nil
		},
	}
	dp := &mockDataProvider{data: "gen1"}
	u := newTestMultiUploader(t, dp, a, b)

	// Cycle 1: b fails, generation incomplete; a must not be blamed.
	err := u.uploadMulti(context.Background())
	if err == nil {
		t.Fatalf("expected incomplete-generation error")
	}
	if !strings.Contains(err.Error(), "b (mock://b)") ||
		strings.Contains(err.Error(), "a (mock://a)") {
		t.Fatalf("error should name only destination b, got: %v", err)
	}
	if calls := a.calls(); len(calls) != 1 {
		t.Fatalf("destination a: expected 1 upload, got %v", calls)
	}
	st, _ := u.Stats()
	if st["generation_complete"] != false {
		t.Fatalf("expected generation_complete false after failure")
	}
	destStats := st["destinations"].([]map[string]any)
	if destStats[1]["last_error"] == "" {
		t.Fatalf("expected destination b last_error to be recorded")
	}
	if destStats[1]["last_uploaded_id"] != "" {
		t.Fatalf("expected failed destination b to have no confirmed ID")
	}

	// Cycle 2, same generation: only b is re-uploaded; a is not touched.
	if err := u.uploadMulti(context.Background()); err != nil {
		t.Fatalf("expected generation to complete on retry, got %v", err)
	}
	if calls := a.calls(); len(calls) != 1 {
		t.Fatalf("destination a should not be re-uploaded, calls: %v", calls)
	}
	if calls := b.calls(); len(calls) != 2 || calls[1] != "1" {
		t.Fatalf("destination b: expected retry of generation 1, got %v", calls)
	}
	if got := string(b.lastData()); got != "gen1" {
		t.Fatalf("destination b: uploaded data %q, want gen1", got)
	}
	st, _ = u.Stats()
	if st["generation_complete"] != true {
		t.Fatalf("expected generation_complete true after recovery")
	}
	destStats = st["destinations"].([]map[string]any)
	if destStats[1]["last_error"] != "" || destStats[1]["last_uploaded_id"] != "1" {
		t.Fatalf("expected destination b to be reconciled, got %+v", destStats[1])
	}

	// Cycle 3, same generation: nothing to upload.
	okBefore := stats.Get(numUploadsOK).(*expvar.Int).Value()
	if err := u.uploadMulti(context.Background()); err != nil {
		t.Fatalf("unexpected error on idle cycle: %v", err)
	}
	if len(a.calls()) != 1 || len(b.calls()) != 2 {
		t.Fatalf("no destination should be uploaded when all are current")
	}
	if stats.Get(numUploadsOK).(*expvar.Int).Value() != okBefore {
		t.Fatalf("expected no new successful uploads on idle cycle")
	}
	if stats.Get(numUploadsSkipped).(*expvar.Int).Value() == 0 {
		t.Fatalf("expected skipped counter to increment")
	}
}

func Test_MultiUploaderCurrentIDReconcile(t *testing.T) {
	ResetStats()
	// a already holds generation 1 remotely; b is unreachable for the probe;
	// c holds an older generation.
	a := &mockDestinationClient{name: "a", currentID: "1"}
	b := &mockDestinationClient{name: "b", currentIDErr: fmt.Errorf("head object failed")}
	c := &mockDestinationClient{name: "c", currentID: "0"}
	dp := &mockDataProvider{data: "gen1"}
	u := newTestMultiUploader(t, dp, a, b, c)

	if err := u.uploadMulti(context.Background()); err != nil {
		t.Fatalf("expected generation to complete, got %v", err)
	}
	if calls := a.calls(); len(calls) != 0 {
		t.Fatalf("destination a already current, should not be uploaded, got %v", calls)
	}
	if calls := b.calls(); len(calls) != 1 || calls[0] != "1" {
		t.Fatalf("destination b: expected upload after probe failure, got %v", calls)
	}
	if calls := c.calls(); len(calls) != 1 || calls[0] != "1" {
		t.Fatalf("destination c: expected upload of current generation, got %v", calls)
	}
	if stats.Get(numUploadsSkippedID).(*expvar.Int).Value() == 0 {
		t.Fatalf("expected skipped-ID counter to increment for destination a")
	}
	if stats.Get(numSumGetFail).(*expvar.Int).Value() == 0 {
		t.Fatalf("expected sum-get-fail counter to increment for destination b")
	}

	// A new generation: all three are behind (a at 1, b at 1, c at 0) and
	// upload generation 2 without any further CurrentID probes.
	dp.data = "gen2"
	dp.lastIndexFn = func() (uint64, error) { return 2, nil }
	if err := u.uploadMulti(context.Background()); err != nil {
		t.Fatalf("expected generation 2 to complete, got %v", err)
	}
	wantCalls := map[string][]string{"a": {"2"}, "b": {"1", "2"}, "c": {"1", "2"}}
	for _, m := range []*mockDestinationClient{a, b, c} {
		if got := fmt.Sprint(m.calls()); got != fmt.Sprint(wantCalls[m.name]) {
			t.Fatalf("destination %s: expected attempts %v, got %v", m.name, wantCalls[m.name], got)
		}
		if got := string(m.lastData()); got != "gen2" {
			t.Fatalf("destination %s: uploaded data %q, want gen2", m.name, got)
		}
		if m.currentIDCalls != 1 {
			t.Fatalf("destination %s: CurrentID should only be called on first reconcile, got %d",
				m.name, m.currentIDCalls)
		}
	}
}

func Test_MultiUploaderCatchesLaggingTargetToLatestGeneration(t *testing.T) {
	ResetStats()
	a := &mockDestinationClient{name: "a", currentIDErr: fmt.Errorf("not found")}
	b := &mockDestinationClient{name: "b", currentIDErr: fmt.Errorf("not found")}
	dp := &mockDataProvider{data: "gen1"}
	u := newTestMultiUploader(t, dp, a, b)

	if err := u.uploadMulti(context.Background()); err != nil {
		t.Fatalf("gen 1: %v", err)
	}

	// Generation 2: a fails and remains at generation 1; b reaches generation 2.
	a.uploadErrFn = func(call int) error { return fmt.Errorf("boom") }
	dp.data = "gen2"
	dp.lastIndexFn = func() (uint64, error) { return 2, nil }
	if err := u.uploadMulti(context.Background()); err == nil {
		t.Fatalf("expected gen 2 to be incomplete")
	}

	// Generation 3: a recovers. Both lagging targets receive the latest
	// generation directly; the missed generation 2 is not replayed.
	a.uploadErrFn = nil
	dp.data = "gen3"
	dp.lastIndexFn = func() (uint64, error) { return 3, nil }
	if err := u.uploadMulti(context.Background()); err != nil {
		t.Fatalf("expected gen 3 to complete, got %v", err)
	}
	if got := fmt.Sprint(a.successIDs()); got != "[1 3]" {
		t.Fatalf("destination a: expected successful generations [1 3], got %s", got)
	}
	if got := fmt.Sprint(b.successIDs()); got != "[1 2 3]" {
		t.Fatalf("destination b: expected successful generations [1 2 3], got %s", got)
	}
	if got := string(a.lastData()); got != "gen3" {
		t.Fatalf("destination a: expected gen3 data, got %q", got)
	}
	st, _ := u.Stats()
	if st["generation_complete"] != true || st["last_generation_id"] != "3" {
		t.Fatalf("unexpected stats: %+v", st)
	}
}

func Test_MultiUploaderContextCancellation(t *testing.T) {
	ResetStats()
	a := &mockDestinationClient{name: "a", currentIDErr: fmt.Errorf("not found")}
	b := &mockDestinationClient{name: "b", currentIDErr: fmt.Errorf("not found")}
	dp := &mockDataProvider{data: "gen1"}
	u := newTestMultiUploader(t, dp, a, b)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := u.uploadMulti(ctx); err == nil {
		t.Fatalf("expected error when context is canceled")
	}
	st, _ := u.Stats()
	if st["generation_complete"] != false {
		t.Fatalf("expected generation to remain incomplete after cancellation")
	}
}

func Test_MultiUploaderInitialStats(t *testing.T) {
	a := &mockDestinationClient{name: "a"}
	u := newTestMultiUploader(t, &mockDataProvider{data: "x"}, a)
	st, err := u.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st["last_generation_id"] != "0" {
		t.Fatalf("expected initial last_generation_id 0, got %v", st["last_generation_id"])
	}
	if st["generation_complete"] != false {
		t.Fatalf("expected initial generation_complete false")
	}
	if st["num_destinations"] != 1 {
		t.Fatalf("expected 1 destination, got %v", st["num_destinations"])
	}
	destStats := st["destinations"].([]map[string]any)
	if destStats[0]["name"] != "a" || destStats[0]["destination"] != "mock://a" {
		t.Fatalf("unexpected destination identity: %+v", destStats[0])
	}
	if destStats[0]["last_uploaded_id"] != "" {
		t.Fatalf("expected empty last_uploaded_id initially")
	}
}
