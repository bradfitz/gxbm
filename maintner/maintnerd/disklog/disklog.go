// Package disklog serves maintner mutation log files from a local directory
// over HTTP, using the same JSON segment index + long-polling protocol that
// NetworkMutationSource expects.
//
// It is analogous to gcslog but reads from DiskMutationLogger's daily
// maintner-YYYY-MM-DD.mutlog files instead of Google Cloud Storage.
package disklog

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gxbm/maintner"
	"github.com/bradfitz/gxbm/maintner/maintpb"
	"google.golang.org/protobuf/proto"
)

// DiskLog implements MutationLogger and serves mutation log segments over HTTP.
var _ maintner.MutationLogger = (*DiskLog)(nil)

// DiskLog reads maintner-*.mutlog files from a directory and serves them as
// numbered segments over HTTP, using the same JSON index and long-polling
// protocol that [maintner.NetworkMutationSource] expects.
//
// It supports two modes:
//
// In writer mode (created via [NewWriterDiskLog]), it wraps a
// [maintner.DiskMutationLogger] and intercepts Log calls for instant
// segment state updates.
//
// In watcher mode (created via [NewDiskLog] + [DiskLog.StartWatching]),
// it polls the filesystem for changes made by an external writer.
type DiskLog struct {
	dir string

	mu      sync.Mutex
	cond    *sync.Cond
	segs    []segment // frozen segments only
	curFile string    // growing segment file path ("" if none)
	curSize int64     // bytes in growing segment
	curHash hash.Hash // incremental SHA-224 of growing segment

	inner *maintner.DiskMutationLogger // non-nil in writer mode
}

type segment struct {
	num    int
	file   string
	size   int64
	sha224 string // lowercase hex
}

// NewDiskLog creates a DiskLog by scanning dir for existing maintner-*.mutlog
// files. The DiskLog starts in read-only mode; call StartWatching to detect
// changes made by an external writer, or use NewWriterDiskLog instead.
func NewDiskLog(dir string) (*DiskLog, error) {
	dl := &DiskLog{dir: dir}
	dl.cond = sync.NewCond(&dl.mu)
	if err := dl.scan(); err != nil {
		return nil, err
	}
	return dl, nil
}

// NewWriterDiskLog creates a DiskLog that wraps a DiskMutationLogger. Each
// call to Log writes through to the underlying logger and instantly updates
// the segment index. No filesystem polling is needed in this mode.
func NewWriterDiskLog(dir string, logger *maintner.DiskMutationLogger) (*DiskLog, error) {
	dl := &DiskLog{dir: dir, inner: logger}
	dl.cond = sync.NewCond(&dl.mu)
	if err := dl.scan(); err != nil {
		return nil, err
	}
	return dl, nil
}

// mutlogFiles returns all maintner-*.mutlog files in dl.dir, sorted lexically.
func (dl *DiskLog) mutlogFiles() ([]string, error) {
	entries, err := os.ReadDir(dl.dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "maintner-") && strings.HasSuffix(name, ".mutlog") {
			files = append(files, filepath.Join(dl.dir, name))
		}
	}
	sort.Strings(files)
	return files, nil
}

// scan reads the directory and initializes segment state.
func (dl *DiskLog) scan() error {
	files, err := dl.mutlogFiles()
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}

	// All but the last are frozen.
	dl.segs = nil
	for i, f := range files[:len(files)-1] {
		fi, err := os.Stat(f)
		if err != nil {
			return err
		}
		h, err := hashFile(f)
		if err != nil {
			return err
		}
		dl.segs = append(dl.segs, segment{
			num:    i,
			file:   f,
			size:   fi.Size(),
			sha224: h,
		})
	}

	// Last file is the growing segment.
	last := files[len(files)-1]
	content, err := os.ReadFile(last)
	if err != nil {
		return err
	}
	dl.curFile = last
	dl.curSize = int64(len(content))
	dl.curHash = sha256.New224()
	dl.curHash.Write(content)
	return nil
}

// hashFile computes the lowercase hex SHA-224 of the entire file.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New224()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// Log implements maintner.MutationLogger. It is only valid when the DiskLog
// was created with NewWriterDiskLog.
func (dl *DiskLog) Log(m *maintpb.Mutation) error {
	if dl.inner == nil {
		panic("disklog: Log called on a read-only DiskLog (use NewWriterDiskLog)")
	}

	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}

	dl.mu.Lock()
	defer dl.mu.Unlock()

	// Detect day rollover: compute what filename DiskMutationLogger would use.
	newFile := filepath.Join(dl.dir, fmt.Sprintf("maintner-%s.mutlog", time.Now().UTC().Format("2006-01-02")))
	if dl.curFile != "" && newFile != dl.curFile {
		// Freeze the current growing segment.
		dl.segs = append(dl.segs, segment{
			num:    len(dl.segs),
			file:   dl.curFile,
			size:   dl.curSize,
			sha224: fmt.Sprintf("%x", dl.curHash.Sum(nil)),
		})
		dl.curFile = newFile
		dl.curSize = 0
		dl.curHash = sha256.New224()
	} else if dl.curFile == "" {
		// First mutation ever.
		dl.curFile = newFile
		dl.curSize = 0
		dl.curHash = sha256.New224()
	}

	// Compute the reclog record bytes to update our hash before writing.
	// reclog.WriteRecord writes: fmt.Fprintf(w, "REC@%x+%x=%s", off, len(data), data)
	hdr := fmt.Sprintf("REC@%x+%x=", dl.curSize, len(data))
	dl.curHash.Write([]byte(hdr))
	dl.curHash.Write(data)
	dl.curSize += int64(len(hdr)) + int64(len(data))

	// Write through to the underlying logger.
	if err := dl.inner.Log(m); err != nil {
		return err
	}

	dl.cond.Broadcast()
	return nil
}

// StartWatching begins a goroutine that polls the filesystem for changes at
// the given interval. This is used in watcher mode when another process writes
// the mutation log files. The goroutine stops when ctx is canceled.
func (dl *DiskLog) StartWatching(ctx context.Context, interval time.Duration) {
	go dl.watchLoop(ctx, interval)
}

func (dl *DiskLog) watchLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dl.poll()
		}
	}
}

func (dl *DiskLog) poll() {
	files, err := dl.mutlogFiles()
	if err != nil {
		log.Printf("disklog: poll error reading dir: %v", err)
		return
	}
	if len(files) == 0 {
		return
	}

	dl.mu.Lock()
	defer dl.mu.Unlock()

	numFrozen := len(dl.segs)
	expectedFiles := numFrozen
	if dl.curFile != "" {
		expectedFiles++
	}

	// Check if new files appeared (day rollover by external writer).
	if len(files) > expectedFiles {
		// Freeze the old growing segment if it existed.
		if dl.curFile != "" {
			dl.segs = append(dl.segs, segment{
				num:    len(dl.segs),
				file:   dl.curFile,
				size:   dl.curSize,
				sha224: fmt.Sprintf("%x", dl.curHash.Sum(nil)),
			})
		}

		// Hash any new frozen segments (files between old frozen count and new last file).
		for i := len(dl.segs); i < len(files)-1; i++ {
			fi, err := os.Stat(files[i])
			if err != nil {
				log.Printf("disklog: poll stat %s: %v", files[i], err)
				return
			}
			h, err := hashFile(files[i])
			if err != nil {
				log.Printf("disklog: poll hash %s: %v", files[i], err)
				return
			}
			dl.segs = append(dl.segs, segment{
				num:    i,
				file:   files[i],
				size:   fi.Size(),
				sha224: h,
			})
		}

		// New growing segment.
		last := files[len(files)-1]
		content, err := os.ReadFile(last)
		if err != nil {
			log.Printf("disklog: poll read %s: %v", last, err)
			return
		}
		dl.curFile = last
		dl.curSize = int64(len(content))
		dl.curHash = sha256.New224()
		dl.curHash.Write(content)
		dl.cond.Broadcast()
		return
	}

	if dl.curFile == "" {
		if len(files) > 0 {
			// First file appeared.
			last := files[len(files)-1]
			content, err := os.ReadFile(last)
			if err != nil {
				log.Printf("disklog: poll read %s: %v", last, err)
				return
			}
			dl.curFile = last
			dl.curSize = int64(len(content))
			dl.curHash = sha256.New224()
			dl.curHash.Write(content)
			dl.cond.Broadcast()
		}
		return
	}

	// Check if the growing segment grew.
	fi, err := os.Stat(dl.curFile)
	if err != nil {
		log.Printf("disklog: poll stat %s: %v", dl.curFile, err)
		return
	}
	newSize := fi.Size()
	if newSize <= dl.curSize {
		return
	}

	// Read the new bytes and feed them to the incremental hash.
	f, err := os.Open(dl.curFile)
	if err != nil {
		log.Printf("disklog: poll open %s: %v", dl.curFile, err)
		return
	}
	defer f.Close()
	if _, err := f.Seek(dl.curSize, io.SeekStart); err != nil {
		log.Printf("disklog: poll seek %s: %v", dl.curFile, err)
		return
	}
	delta := make([]byte, newSize-dl.curSize)
	if _, err := io.ReadFull(f, delta); err != nil {
		log.Printf("disklog: poll read delta %s: %v", dl.curFile, err)
		return
	}
	dl.curHash.Write(delta)
	dl.curSize = newSize
	dl.cond.Broadcast()
}

// RegisterHandlers registers HTTP handlers on mux for serving the segment
// index and segment data. The prefix should be like "/logs/ts-v1".
//
// It registers:
//
//	prefix + "/" for the JSON segment index (with long-polling)
//	prefix + "/<N>" for individual segment data
func (dl *DiskLog) RegisterHandlers(mux *http.ServeMux, prefix string) {
	mux.HandleFunc(prefix+"/", func(w http.ResponseWriter, r *http.Request) {
		// "/logs/ts-v1/" is the index, "/logs/ts-v1/0" etc. are segments.
		rest := strings.TrimPrefix(r.URL.Path, prefix+"/")
		if rest == "" {
			dl.serveJSONLogsIndex(w, r, prefix)
			return
		}
		dl.serveLogFile(w, r, prefix, rest)
	})
}

func (dl *DiskLog) serveJSONLogsIndex(w http.ResponseWriter, r *http.Request, prefix string) {
	if r.Method != "GET" && r.Method != "HEAD" {
		http.Error(w, "bad method", http.StatusBadRequest)
		return
	}

	// Long poll if request contains non-zero waitsizenot parameter.
	if s := r.FormValue("waitsizenot"); s != "" {
		oldSize, err := strconv.ParseInt(s, 10, 64)
		if err != nil || oldSize < 0 {
			http.Error(w, "bad waitsizenot", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 55*time.Second)
		defer cancel()
		changed := dl.waitSizeNot(ctx, oldSize)
		if !changed {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	segs := dl.getJSONLogs(prefix)

	w.Header().Set("Content-Type", "application/json")
	var sum int64
	for _, seg := range segs {
		sum += seg.Size
	}
	w.Header().Set("X-Sum-Segment-Size", fmt.Sprint(sum))
	body, _ := json.MarshalIndent(segs, "", "\t")
	w.Write(body)
}

func (dl *DiskLog) getJSONLogs(prefix string) []maintner.LogSegmentJSON {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	segs := make([]maintner.LogSegmentJSON, 0, len(dl.segs)+1)
	for _, seg := range dl.segs {
		segs = append(segs, maintner.LogSegmentJSON{
			Number: seg.num,
			Size:   seg.size,
			SHA224: seg.sha224,
			URL:    fmt.Sprintf("%s/%d", prefix, seg.num),
			Frozen: true,
		})
	}
	if dl.curSize > 0 {
		segs = append(segs, maintner.LogSegmentJSON{
			Number: len(dl.segs),
			Size:   dl.curSize,
			SHA224: fmt.Sprintf("%x", dl.curHash.Sum(nil)),
			URL:    fmt.Sprintf("%s/%d", prefix, len(dl.segs)),
		})
	}
	return segs
}

func (dl *DiskLog) waitSizeNot(ctx context.Context, v int64) (changed bool) {
	returned := make(chan struct{})
	defer close(returned)
	go dl.waitSizeNotAwaitContext(ctx, returned)
	dl.mu.Lock()
	defer dl.mu.Unlock()
	for {
		if dl.sumSizeLocked() != v {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		default:
			dl.cond.Wait()
		}
	}
}

func (dl *DiskLog) waitSizeNotAwaitContext(ctx context.Context, returned <-chan struct{}) {
	select {
	case <-ctx.Done():
		dl.cond.Broadcast()
	case <-returned:
	}
}

func (dl *DiskLog) sumSizeLocked() int64 {
	var sum int64
	for _, seg := range dl.segs {
		sum += seg.size
	}
	sum += dl.curSize
	return sum
}

func (dl *DiskLog) serveLogFile(w http.ResponseWriter, r *http.Request, prefix, rest string) {
	if r.Method != "GET" && r.Method != "HEAD" {
		http.Error(w, "bad method", http.StatusBadRequest)
		return
	}

	num, err := strconv.Atoi(rest)
	if err != nil {
		http.Error(w, "bad segment number", http.StatusBadRequest)
		return
	}

	dl.mu.Lock()
	curNum := len(dl.segs) // growing segment number

	if num < 0 || num > curNum {
		dl.mu.Unlock()
		http.Error(w, "segment not found", http.StatusNotFound)
		return
	}

	var file string
	var size int64
	if num < curNum {
		// Frozen segment.
		file = dl.segs[num].file
		size = dl.segs[num].size
	} else {
		// Growing segment.
		file = dl.curFile
		size = dl.curSize
	}
	dl.mu.Unlock()

	if file == "" || size == 0 {
		http.Error(w, "segment not found", http.StatusNotFound)
		return
	}

	f, err := os.Open(file)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "application/octet-stream")

	// For Range requests (partial downloads), use ServeContent which
	// handles Range/Content-Range headers. Gzip is incompatible with
	// Range since the byte offsets refer to uncompressed data.
	if r.Header.Get("Range") != "" {
		http.ServeContent(w, r, "", time.Time{}, io.NewSectionReader(f, 0, size))
		return
	}

	// For full downloads, gzip if the client accepts it.
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		http.ServeContent(w, r, "", time.Time{}, io.NewSectionReader(f, 0, size))
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	w.WriteHeader(http.StatusOK)
	gz := gzip.NewWriter(w)
	if _, err := io.CopyN(gz, f, size); err != nil {
		panic(http.ErrAbortHandler)
	}
	gz.Close()
}
