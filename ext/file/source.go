package file

import (
	"bufio"
	"io"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/component/source"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// FileSource is a source that reads data from a file.
type FileSource struct {
	*source.CommonSource
	f *os.File
}

var _ flow.Source = (*FileSource)(nil)

// NewSource creates a new file source.
func NewSource(l *slog.Logger, path string, opts ...option.Option) (*FileSource, error) {
	// create commonSource
	commonSource := source.NewCommonSource(l, opts...)
	// open file
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// create source
	fs := &FileSource{
		CommonSource: commonSource,
		f:            f,
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
	// read file
	r := bufio.NewReader(fs.f)
	for {
		// read line
		line, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				fs.Logger.Debug("source(file): end of file")
				break
			}
			fs.Logger.Error(err.Error())
			fs.SetError(err)
			continue
		}
		// send to channel
		fs.Send(line)
	}
}
