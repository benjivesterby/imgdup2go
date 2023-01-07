package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Nr90/imgsim"
	"github.com/pkg/errors"
	"github.com/rif/imgdup2go/hasher"
	"github.com/rivo/duplo"
	"github.com/vbauerster/mpb/v5"
	"github.com/vbauerster/mpb/v5/decor"
	"go.atomizer.io/stream"
)

var (
	extensions  = map[string]func(io.Reader) (image.Image, error){"jpg": jpeg.Decode, "jpeg": jpeg.Decode, "png": png.Decode, "gif": gif.Decode}
	algo        = flag.String("algo", "avg", "algorithm for image hashing fmiq|avg|diff")
	sensitivity = flag.Int("sensitivity", 0, "the sensitivity treshold (the lower, the better the match (can be negative)) - fmiq algorithm only")
	searchPath  = flag.String("path", ".", "the path to search the images")
	dryRun      = flag.Bool("dryrun", false, "only print found matches")
	undo        = flag.Bool("undo", false, "restore removed duplicates")
	recurse     = flag.Bool("r", false, "go through subdirectories as well")
)

const (
	DUPLICATES   = "duplicates"
	keepPrefix   = "_KEPT_"
	deletePrefix = "_GONE_"
)

type imgInfo struct {
	fileInfo string
	res      int
}

// CopyFile copies a file from src to dst. If src and dst files exist, and are
// the same, then return success. Otherise, attempt to create a hard link
// between the two files. If that fail, copy the file contents from src to dst.
func CopyFile(src, dst string) (err error) {
	sfi, err := os.Stat(src)
	if err != nil {
		return
	}
	if !sfi.Mode().IsRegular() {
		// cannot copy non-regular files (e.g., directories,
		// symlinks, devices, etc.)
		return fmt.Errorf("CopyFile: non-regular source file %s (%q)", sfi.Name(), sfi.Mode().String())
	}
	dfi, err := os.Stat(dst)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
	} else {
		if !(dfi.Mode().IsRegular()) {
			return fmt.Errorf("CopyFile: non-regular destination file %s (%q)", dfi.Name(), dfi.Mode().String())
		}
		if os.SameFile(sfi, dfi) {
			return
		}
	}
	if err = os.Link(src, dst); err == nil {
		return
	}
	err = copyFileContents(src, dst)
	return
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer checkClose(in, &err)
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer checkClose(out, &err)
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flag.Parse()

	*sensitivity -= 100
	store := createEmptyStore()

	matcher := &Matcher{
		store: store,
	}

	s := stream.Scaler[string, struct{}]{
		Wait: time.Millisecond,
		Life: time.Second / 2,
		Fn:   matcher.Match,
	}

	out, err := s.Exec(ctx, filenames(ctx, *searchPath))
	if err != nil {
		log.Fatal(err)
	}

	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	total := 0

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-out:
			if !ok {
				return
			}
			total++
		case <-tick.C:
			log.Printf("processed: %d | total duplicates: %d", total, matcher.count)
		}
	}
}

func filenames(ctx context.Context, dir string) <-chan string {
	out := make(chan string)

	go func() {
		defer close(out)

		files, err := os.ReadDir(dir)
		if err != nil {
			fmt.Println(err)
			return
		}

		wg := sync.WaitGroup{}
		for _, file := range files {
			if !file.IsDir() {
				i, err := file.Info()
				if err != nil {
					continue
				}

				select {
				case <-ctx.Done():
					return
				case out <- path.Join(dir, i.Name()):
				}

				continue
			}

			i, err := file.Info()
			if err != nil {
				return
			}

			wg.Add(1)
			go func(d os.FileInfo) {
				defer wg.Done()

				stream.Pipe(ctx, filenames(ctx, path.Join(dir, d.Name())), out)
			}(i)
		}

		wg.Wait()
	}()

	return out
}

func findFilesToCompare(logger *log.Logger) []string {
	var files []string
	total := int64(0)
	err := filepath.Walk(*searchPath, func(currentPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// logger.Printf("file %s", currentPath)
		if !info.IsDir() {
			total++
			logger.Printf("Found file: %v", info.Name())
			files = append(files, currentPath)
		}
		if info.IsDir() && !*recurse && currentPath != *searchPath {
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		logger.Printf("Failed to read files: %v", err)
	}
	logger.Printf("Found %d files\n", total)

	return files
}

func createEmptyStore() hasher.Store {
	// Create an empty store.
	var store hasher.Store
	switch *algo {
	case "fmiq":
		store = hasher.NewDuploStore(*sensitivity)
	default:
		store = hasher.NewImgsimStore()
	}
	return store
}

type Matcher struct {
	store   hasher.Store
	storeMu sync.Mutex
	count   int
}

func (m *Matcher) Match(ctx context.Context, path string) (struct{}, bool) {
	ext := filepath.Ext(path)
	if len(ext) == 0 {
		return struct{}{}, false
	}

	if _, ok := extensions[ext[1:]]; !ok {
		return struct{}{}, false
	}

	format, err := getImageFormat(path)
	if err != nil {
		return struct{}{}, false
	}

	if decodeFunc, ok := extensions[format]; ok {
		//fmt.Printf("decoding %s\n", path)
		file, err := os.Open(path)
		if err != nil {
			//fmt.Printf("%s\n", errors.WithMessagef(err, "%s", path))
			return struct{}{}, false
		}
		defer checkClose(file, &err)

		img, err := decodeFunc(file)
		if err != nil {
			//fmt.Printf("%s\n", errors.WithMessagef(err, "ignoring %s", path))
			return struct{}{}, false
		}
		b := img.Bounds()
		res := b.Dx() * b.Dy()

		hash := createImageHash(img)

		m.storeMu.Lock()
		defer m.storeMu.Unlock()

		match := m.store.Query(hash)
		if match != nil {
			m.count++
			matchedImgInfo, ok := match.(*imgInfo)
			if !ok {
				//fmt.Printf("unexpected type %T\n", match)
				return struct{}{}, false
			}

			matchedFile := matchedImgInfo.fileInfo
			fmt.Printf("%s matches: %s\n", path, matchedFile)
		}

		m.store.Add(&imgInfo{fileInfo: path, res: res}, hash)

		return struct{}{}, true
	}

	return struct{}{}, false
}

func handleFile(store hasher.Store, currentFile string, logger *log.Logger, dst string) (err error) {
	ext := filepath.Ext(currentFile)
	if len(ext) > 1 {
		ext = ext[1:]
	}
	if _, ok := extensions[ext]; !ok {
		return nil
	}
	format, err := getImageFormat(currentFile)
	if err != nil {
		return err
	}

	if decodeFunc, ok := extensions[format]; ok {
		logger.Printf("decoding %s", currentFile)
		file, err := os.Open(currentFile)
		if err != nil {
			return errors.WithMessagef(err, "%s", currentFile)
		}
		defer checkClose(file, &err)

		img, err := decodeFunc(file)
		if err != nil {
			return errors.WithMessagef(err, "ignoring %s", currentFile)
		}
		b := img.Bounds()
		res := b.Dx() * b.Dy()

		hash := createImageHash(img)
		match := store.Query(hash)
		if match != nil {
			matchedImgInfo := match.(*imgInfo)
			matchedFile := matchedImgInfo.fileInfo
			logger.Printf("%s matches: %s\n", currentFile, matchedFile)
		}

		store.Add(&imgInfo{fileInfo: currentFile, res: res}, hash)
	}
	return err
}

func createFileHash(filename string, fi string) (string, error) {
	md5Hasher := md5.New()
	_, err := md5Hasher.Write([]byte(filename + fi))
	if err != nil {
		return "", errors.WithMessagef(err, "Failed to write to %q", filename+fi)
	}
	sum := hex.EncodeToString(md5Hasher.Sum(nil))[:5]
	return sum, nil
}

func checkClose(c io.Closer, err *error) {
	cErr := c.Close()
	if *err == nil {
		*err = cErr
	}
}

func createImageHash(img image.Image) interface{} {
	var hash interface{}
	switch *algo {
	case "fmiq":
		hash, _ = duplo.CreateHash(img)
	case "avg":
		hash = imgsim.AverageHash(img)
	case "diff":
		hash = imgsim.DifferenceHash(img)
	default:
		hash = imgsim.AverageHash(img)
	}
	return hash
}

func getImageFormat(fn string) (format string, err error) {
	file, err := os.Open(fn)
	if err != nil {
		return "", errors.WithMessagef(err, "%s", fn)
	}
	defer checkClose(file, &err)
	_, format, err = image.DecodeConfig(file)
	if err != nil {
		return "", errors.WithMessagef(err, "%s", fn)
	}
	return format, nil
}

func createProgressBar(total int64) (*mpb.Progress, *mpb.Bar) {
	p := mpb.New(
		// override default (80) width
		mpb.WithWidth(64),
		// override default 120ms refresh rate
		mpb.WithRefreshRate(180*time.Millisecond),
	)

	name := "Processed Images:"
	// Add a bar
	// You're not limited to just a single bar, add as many as you need
	bar := p.AddBar(total,
		// override default "[=>-]" format
		mpb.BarStyle("╢▌▌░╟"),
		// Prepending decorators
		mpb.PrependDecorators(
			// display our name with one space on the right
			decor.Name(name, decor.WC{W: len(name) + 1, C: decor.DidentRight}),
			decor.CountersNoUnit("%d/%d"),
			decor.OnComplete(
				// ETA decorator with ewma age of 60, and width reservation of 4
				decor.EwmaETA(decor.ET_STYLE_GO, 60, decor.WC{W: 4}), " done",
			),
		),
		// Appending decorators
		mpb.AppendDecorators(
			// Percentage decorator with minWidth and no extra config
			decor.Percentage(),
			decor.Name(" - "),
			decor.NewAverageETA(decor.ET_STYLE_GO, time.Now(), decor.FixedIntervalTimeNormalizer(5)),
		),
	)
	return p, bar
}

func handleUndo(logger *log.Logger, dst string) {
	files, err := ioutil.ReadDir(dst)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		if strings.Contains(f.Name(), keepPrefix) {
			if *dryRun {
				logger.Println("removing ", f.Name())
			} else {
				err := os.Remove(filepath.Join(dst, f.Name()))
				if err != nil {
					logger.Fatalf("Failed to remove file %q: %v", f.Name(), err)
				}
			}
		}
		if strings.Contains(f.Name(), deletePrefix) {
			if *dryRun {
				logger.Printf("moving %s to %s\n ", filepath.Join(dst, f.Name()), filepath.Join(*searchPath, f.Name()[13:]))
			} else {
				err := os.Rename(filepath.Join(dst, f.Name()), filepath.Join(*searchPath, f.Name()[13:]))
				if err != nil {
					logger.Fatalf("Failed to rename file %q: %v", f.Name(), err)
				}
			}
		}
	}
	if *dryRun {
		logger.Print("removing directory: ", dst)
	} else if err := os.Remove(dst); err != nil {
		logger.Print("could not remove duplicates folder: ", err)
	}
	os.Exit(0)
}
