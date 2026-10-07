package backup

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rqlite/rqlite/v10/db/humanize"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/internal/progress"
)

// StorageClient is an interface for uploading data to a storage service.
type StorageClient interface {
	// Upload uploads the data from the given reader to the storage service.
	// id is a identifier for the data, and will be stored along with
	// the data in the storage service.
	Upload(ctx context.Context, reader io.Reader, id string) error

	// CurrentID returns the ID of the data in the Storage service.
	// It is always read from the Storage service, and a cached
	// value is never returned.
	CurrentID(ctx context.Context) (string, error)

	fmt.Stringer
}

// DataProvider is an interface for providing data to be uploaded. The Uploader
// service will call Provide() to have the data-for-upload to be written to the
// to the file specified by path.
type DataProvider interface {
	// LastIndex returns the cluster-wide index the data managed by the DataProvider was
	// last modified by.
	LastIndex() (uint64, error)

	// Provide writes the data-for-upload to the writer.
	Provide(w io.WriteSeeker) error
}

// stats captures stats for the Uploader service. Counters aggregate upload
// attempts across all configured destinations.
var stats *expvar.Map

const (
	numUploadsOK        = "num_uploads_ok"
	numUploadsFail      = "num_uploads_fail"
	numUploadsSkipped   = "num_uploads_skipped"
	numUploadsSkippedID = "num_uploads_skipped_id"
	numSumGetFail       = "num_sum_get_fail"
	totalUploadBytes    = "total_upload_bytes"
	lastUploadBytes     = "last_upload_bytes"
)

func init() {
	stats = expvar.NewMap("uploader")
	ResetStats()
}

// ResetStats resets the expvar stats for this module. Mostly for test purposes.
func ResetStats() {
	stats.Init()
	stats.Add(numUploadsOK, 0)
	stats.Add(numUploadsFail, 0)
	stats.Add(numUploadsSkipped, 0)
	stats.Add(numUploadsSkippedID, 0)
	stats.Add(numSumGetFail, 0)
	stats.Add(totalUploadBytes, 0)
	stats.Add(lastUploadBytes, 0)
}

// uploadTarget tracks the upload progress of a single backup destination.
type uploadTarget struct {
	name   string
	client StorageClient

	// reconciled records whether this destination's state has been read
	// from the backing store via CurrentID at least once. confirmedID is
	// the generation ID known to be present there (it may be empty when
	// the destination holds nothing).
	reconciled         bool
	confirmedID        string
	lastErr            string
	lastUploadTime     time.Time
	lastUploadDuration time.Duration
	lastUploadBytes    int64
	uploadsOK          int64
	uploadsFail        int64
	skippedID          int64
	sumGetFail         int64
}

func newUploadTarget(dest Destination) *uploadTarget {
	return &uploadTarget{
		name:   dest.Name,
		client: dest.Client,
	}
}

func (t *uploadTarget) label() string {
	return fmt.Sprintf("%s (%s)", t.name, t.client)
}

// Uploader is a service that periodically uploads data to one or more storage
// services. When configured with multiple destinations, each backup
// generation is pushed to every destination, and the generation is only
// considered complete once every destination has it.
type Uploader struct {
	storageClient StorageClient
	dataProvider  DataProvider
	interval      time.Duration

	targets      []*uploadTarget
	renderSingle bool // render the legacy single-destination logs and Stats

	logger             *log.Logger
	mu                 sync.RWMutex
	lastUploadTime     time.Time
	lastUploadDuration time.Duration

	lastIndex uint64 // The last index of the data most-recently uploaded (single target).

	// Multi-destination state.
	lastGenerationID   uint64
	generationComplete bool
}

// NewUploader creates a new Uploader service for a single storage destination.
// Its behavior is identical to the version-1 single-target auto-backup
// configuration.
func NewUploader(storageClient StorageClient, dataProvider DataProvider, interval time.Duration) *Uploader {
	return &Uploader{
		storageClient: storageClient,
		dataProvider:  dataProvider,
		interval:      interval,
		targets:       []*uploadTarget{newUploadTarget(Destination{Name: storageClient.String(), Client: storageClient})},
		renderSingle:  true,
		logger:        log.New(os.Stderr, "[uploader] ", log.LstdFlags),
	}
}

// NewUploaderMulti creates a new Uploader service that fans every backup
// generation out to all the given destinations. A backup generation is only
// reported as complete once it has been uploaded to every destination; on
// subsequent runs only destinations that are behind are uploaded to.
func NewUploaderMulti(destinations []Destination, dataProvider DataProvider, interval time.Duration) (*Uploader, error) {
	if len(destinations) == 0 {
		return nil, fmt.Errorf("at least one backup destination is required")
	}
	targets := make([]*uploadTarget, 0, len(destinations))
	for _, dest := range destinations {
		targets = append(targets, newUploadTarget(dest))
	}
	return &Uploader{
		dataProvider: dataProvider,
		interval:     interval,
		targets:      targets,
		renderSingle: false,
		logger:       log.New(os.Stderr, "[uploader] ", log.LstdFlags),
	}, nil
}

// Start starts the Uploader service.
func (u *Uploader) Start(ctx context.Context, isUploadEnabled func() bool) chan struct{} {
	doneCh := make(chan struct{})
	if isUploadEnabled == nil {
		isUploadEnabled = func() bool { return true }
	}

	if u.renderSingle {
		u.logger.Printf("starting upload to %s every %s", u.storageClient, u.interval)
	} else {
		u.logger.Printf("starting upload to %d destinations every %s", len(u.targets), u.interval)
		for _, t := range u.targets {
			u.logger.Printf("backup destination %q: %s", t.name, t.client)
		}
	}
	ticker := time.NewTicker(u.interval)
	go func() {
		defer ticker.Stop()
		defer close(doneCh)
		for {
			select {
			case <-ctx.Done():
				u.logger.Println("upload service shutting down")
				return
			case <-ticker.C:
				if !isUploadEnabled() {
					continue
				}
				if u.renderSingle {
					if err := u.uploadSingle(ctx); err != nil {
						u.logger.Printf("failed to upload to %s: %v", u.storageClient, err)
					}
					continue
				}
				if err := u.uploadMulti(ctx); err != nil {
					u.logger.Println(err)
				}
			}
		}
	}()
	return doneCh
}

// Stats returns the stats for the Uploader service.
func (u *Uploader) Stats() (map[string]any, error) {
	if u.renderSingle {
		return u.statsSingle(), nil
	}
	return u.statsMulti(), nil
}

func (u *Uploader) statsSingle() map[string]any {
	u.mu.RLock()
	defer u.mu.RUnlock()
	status := map[string]any{
		"upload_destination":   u.storageClient.String(),
		"upload_interval":      u.interval.String(),
		"last_upload_time":     u.lastUploadTime.Format(time.RFC3339),
		"last_upload_duration": u.lastUploadDuration.String(),
		"last_index":           strconv.FormatUint(u.lastIndex, 10),
	}
	return status
}

func (u *Uploader) statsMulti() map[string]any {
	u.mu.RLock()
	defer u.mu.RUnlock()
	status := map[string]any{
		"upload_interval":      u.interval.String(),
		"last_generation_id":   strconv.FormatUint(u.lastGenerationID, 10),
		"generation_complete":  u.generationComplete,
		"last_upload_time":     u.lastUploadTime.Format(time.RFC3339),
		"last_upload_duration": u.lastUploadDuration.String(),
		"num_destinations":     len(u.targets),
	}
	destinations := make([]map[string]any, 0, len(u.targets))
	for _, t := range u.targets {
		destinations = append(destinations, map[string]any{
			"name":                 t.name,
			"destination":          t.client.String(),
			"last_uploaded_id":     t.confirmedID,
			"last_upload_time":     t.lastUploadTime.Format(time.RFC3339),
			"last_upload_duration": t.lastUploadDuration.String(),
			"last_upload_bytes":    t.lastUploadBytes,
			"uploads_ok":           t.uploadsOK,
			"uploads_fail":         t.uploadsFail,
			"last_error":           t.lastErr,
		})
	}
	status["destinations"] = destinations
	return status
}

func (u *Uploader) uploadSingle(ctx context.Context) error {
	var err error
	var li uint64

	li, err = u.dataProvider.LastIndex()
	if err != nil {
		return err
	}
	if li <= u.lastIndex {
		stats.Add(numUploadsSkipped, 1)
		return nil
	}

	// Create a temporary file for the data to be uploaded
	fd, err := tempFD()
	if err != nil {
		return err
	}
	defer fsutil.Remove(fd.Name())
	defer fd.Close()

	if err := u.dataProvider.Provide(fd); err != nil {
		return err
	}

	if u.lastIndex == 0 {
		// No last index, so this must be the first upload since this
		// uploader started. Double-check that we really need to upload.
		currID, err := u.storageClient.CurrentID(ctx)
		if err != nil {
			stats.Add(numSumGetFail, 1)
			u.logger.Printf("failed to get current ID from %s: %v", u.storageClient, err)
		} else if currID == strconv.FormatUint(li, 10) {
			stats.Add(numUploadsSkippedID, 1)
			return nil
		}
	}

	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		return err
	}
	cr := progress.NewCountingReader(fd)
	startTime := time.Now()
	err = u.storageClient.Upload(ctx, cr, strconv.FormatUint(li, 10))
	if err != nil {
		stats.Add(numUploadsFail, 1)
		return err
	}

	// Successful upload!
	uploadBytes := cr.Count()
	uploadDuration := time.Since(startTime)
	u.mu.Lock()
	u.lastIndex = li
	u.lastUploadTime = time.Now()
	u.lastUploadDuration = uploadDuration
	u.mu.Unlock()
	stats.Add(numUploadsOK, 1)
	stats.Add(totalUploadBytes, uploadBytes)
	stats.Get(lastUploadBytes).(*expvar.Int).Set(uploadBytes)
	u.logger.Printf("completed auto upload of %s to %s in %s",
		humanize.Bytes(uint64(uploadBytes)),
		u.storageClient, uploadDuration)
	return nil
}

// uploadMulti pushes the current backup generation to every destination that
// does not already have it. Destinations are reconciled independently: a
// failure at one destination does not prevent uploads to the others, and only
// destinations that are behind are uploaded to. The generation is complete
// only when all destinations end the cycle with its ID.
func (u *Uploader) uploadMulti(ctx context.Context) error {
	li, err := u.dataProvider.LastIndex()
	if err != nil {
		return err
	}
	id := strconv.FormatUint(li, 10)

	u.mu.Lock()
	u.lastGenerationID = li
	u.mu.Unlock()

	// Reconcile destinations whose state is still unknown by reading the ID
	// stored at the destination.
	u.reconcileDestinations(ctx, id)

	// Determine which destinations are behind this generation.
	lagging := make([]*uploadTarget, 0)
	u.mu.RLock()
	for _, t := range u.targets {
		if t.confirmedID != id {
			lagging = append(lagging, t)
		}
	}
	u.mu.RUnlock()
	if len(lagging) == 0 {
		stats.Add(numUploadsSkipped, 1)
		u.mu.Lock()
		u.generationComplete = true
		u.mu.Unlock()
		return nil
	}

	// Build the backup for this generation exactly once, then fan it out.
	fd, err := tempFD()
	if err != nil {
		return err
	}
	defer fsutil.Remove(fd.Name())
	defer fd.Close()

	if err := u.dataProvider.Provide(fd); err != nil {
		return err
	}
	fi, err := fd.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()

	// Upload to every lagging destination in parallel. Each goroutine gets
	// its own reader so the temporary-file offset is never shared.
	startTime := time.Now()
	var wg sync.WaitGroup
	for _, t := range lagging {
		wg.Add(1)
		go func(t *uploadTarget) {
			defer wg.Done()
			cr := progress.NewCountingReader(io.NewSectionReader(fd, 0, size))
			uploadStart := time.Now()
			uploadErr := t.client.Upload(ctx, cr, id)
			cycleDuration := time.Since(uploadStart)

			u.mu.Lock()
			defer u.mu.Unlock()
			if uploadErr != nil {
				stats.Add(numUploadsFail, 1)
				t.uploadsFail++
				t.lastErr = uploadErr.Error()
				t.lastUploadTime = time.Now()
				u.logger.Printf("failed to upload generation %s to destination %s: %v", id, t.label(), uploadErr)
				return
			}
			stats.Add(numUploadsOK, 1)
			stats.Add(totalUploadBytes, cr.Count())
			t.uploadsOK++
			t.reconciled = true
			t.confirmedID = id
			t.lastErr = ""
			t.lastUploadBytes = cr.Count()
			t.lastUploadTime = time.Now()
			t.lastUploadDuration = cycleDuration
			u.logger.Printf("completed upload of generation %s, %s, to destination %s in %s",
				id, humanize.Bytes(uint64(cr.Count())), t.label(), cycleDuration)
		}(t)
	}
	wg.Wait()

	// Aggregate cycle stats and work out whether this generation is complete.
	u.mu.Lock()
	var cycleBytes int64
	missing := make([]string, 0)
	for _, t := range u.targets {
		if t.confirmedID == id {
			cycleBytes += t.lastUploadBytes
		} else {
			missing = append(missing, t.label())
		}
	}
	u.generationComplete = len(missing) == 0
	u.lastUploadTime = time.Now()
	u.lastUploadDuration = time.Since(startTime)
	u.mu.Unlock()
	stats.Get(lastUploadBytes).(*expvar.Int).Set(cycleBytes)

	if !u.generationComplete {
		return fmt.Errorf("backup generation %s not complete, destinations missing this generation: %s",
			id, strings.Join(missing, ", "))
	}
	u.logger.Printf("backup generation %s uploaded to all %d destinations", id, len(u.targets))
	return nil
}

// reconcileDestinations queries destinations that have not yet been
// reconciled for the ID they currently hold. A destination holding the
// current generation is marked current; a reconciliation error (e.g. the
// destination is unreachable) leaves the destination unreconciled, so it is
// uploaded to this cycle. Reconciliation errors never block the other
// destinations.
func (u *Uploader) reconcileDestinations(ctx context.Context, id string) {
	var wg sync.WaitGroup
	for _, t := range u.targets {
		u.mu.RLock()
		known := t.reconciled
		u.mu.RUnlock()
		if known {
			continue
		}
		wg.Add(1)
		go func(t *uploadTarget) {
			defer wg.Done()
			currID, err := t.client.CurrentID(ctx)
			u.mu.Lock()
			defer u.mu.Unlock()
			if err != nil {
				stats.Add(numSumGetFail, 1)
				t.sumGetFail++
				t.lastErr = err.Error()
				u.logger.Printf("failed to get current ID from destination %s: %v", t.label(), err)
				return
			}
			t.reconciled = true
			t.confirmedID = currID
			t.lastErr = ""
			if currID == id {
				stats.Add(numUploadsSkippedID, 1)
				t.skippedID++
			}
		}(t)
	}
	wg.Wait()
}

func tempFD() (*os.File, error) {
	return os.CreateTemp("", "rqlite-upload")
}
