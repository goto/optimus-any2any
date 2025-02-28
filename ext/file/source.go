package file

import (
	"bufio"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"

	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/component/source"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// FileSource is a source that reads data from a file.
type FileSource struct {
	*source.CommonSource
	files []*os.File
}

var _ flow.Source = (*FileSource)(nil)

// NewSource creates a new file source.
func NewSource(l *slog.Logger, uri string, opts ...option.Option) (*FileSource, error) {
	// create commonSource
	commonSource := source.NewCommonSource(l, opts...)
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
		CommonSource: commonSource,
		files:        files,
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
		r := bufio.NewReader(f)
		for {
			// read line
			line, _, err := r.ReadLine()
			if err != nil {
				if err == io.EOF {
					fs.Logger.Debug("source(file): end of file")
					break
				}
				fs.Logger.Error(err.Error())
				fs.SetError(errors.WithStack(err))
				continue
			}
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
