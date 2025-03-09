package file

import (
	"bufio"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// FileSource is a source that reads data from a file.
type FileSource struct {
	*common.Source
	files []*os.File
}

var _ flow.Source = (*FileSource)(nil)

// NewSource creates a new file common.
func NewSource(l *slog.Logger, uri string, opts ...common.Option) (*FileSource, error) {
	// create commonSource
	commonSource := common.NewSource(l, opts...)
	// open file
	sourceURI, err := url.Parse(uri)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if sourceURI.Scheme != "file" {
		return nil, errors.New("source(file): invalid scheme")
	}
	f, err := os.Open(sourceURI.Path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	fileStat, err := f.Stat()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// prepare files
	var files []*os.File
	if fileStat.IsDir() {
		// read all files in the directory recursively
		files, err = readFiles(sourceURI.Path)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	} else {
		files = []*os.File{f}
	}
	// create source
	fs := &FileSource{
		Source: commonSource,
		files:  files,
	}

	// add clean func
	commonSource.AddCleanFunc(func() {
		commonSource.Logger.Debug("source(file): close file")
		f.Close()
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSource.RegisterProcess(fs.process)

	return fs, nil
}

// process reads data from the file and sends it to the channel.
func (fs *FileSource) process() {
	// read files
	for _, f := range fs.files {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			// read line
			raw := sc.Bytes()
			line := make([]byte, len(raw)) // Important: make a copy of the line before sending
			copy(line, raw)
			fs.Logger.Debug(fmt.Sprintf("source(file): read line: %s", string(line)))
			// send to channel
			fs.Send(line)
		}
	}
}

func readFiles(dir string) ([]*os.File, error) {
	// read all files in the directory recursively
	files := make([]*os.File, 0)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		files = append(files, f)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
