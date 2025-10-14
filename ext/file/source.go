package file

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// FileSource is a source that reads data from a file.
type FileSource struct {
	common.Source
	Readers []io.ReadCloser
}

var _ flow.Source = (*FileSource)(nil)

// NewSource creates a new file common.
func NewSource(commonSource common.Source, uri string) (*FileSource, error) {
	// open file
	sourceURI, err := url.Parse(uri)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if sourceURI.Scheme != "file" {
		return nil, fmt.Errorf("invalid scheme: %s", sourceURI.Scheme)
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
	var files []io.ReadCloser
	if fileStat.IsDir() {
		// read all files in the directory recursively
		ff, err := ReadFiles(sourceURI.Path)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		for _, f := range ff {
			files = append(files, nio.NewReader(f, buffer.New(32*1024)))
		}
	} else {
		files = append(files, nio.NewReader(f, buffer.New(32*1024)))
	}
	// create source
	fs := &FileSource{
		Source:  commonSource,
		Readers: files,
	}

	// add clean func
	commonSource.AddCleanFunc(func() error {
		fs.Logger().Info(fmt.Sprintf("close files"))
		for _, f := range files {
			f.Close()
		}
		return nil
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSource.RegisterProcess(fs.Process)

	return fs, nil
}

// Process reads data from the file and sends it to the channel.
func (fs *FileSource) Process() error {
	// read files
	for _, f := range fs.Readers {
		_ = fs.DryRunable(func() error {
			reader := bufio.NewReader(f)
			for {
				raw, err := reader.ReadBytes('\n')
				if len(raw) > 0 && raw[0] != '\n' {
					line := make([]byte, len(raw))
					copy(line, raw)

					record := model.NewRecord()
					if err := json.Unmarshal(line, &record); err != nil {
						return errors.WithStack(fmt.Errorf("failed to unmarshal record: %w", err))
					}
					// send to channel
					fs.SendRecord(record)
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.WithStack(err)
				}
			}
			return nil
		})
	}
	return nil
}

func ReadFiles(dir string) ([]*os.File, error) {
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
