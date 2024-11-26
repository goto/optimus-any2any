package file

import (
	"bufio"
	"io"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/goto/optimus-any2any/pkg/source"
)

// FileSource is a source that reads data from a file.
type FileSource struct {
	*source.Common
	logger *slog.Logger
	f      *os.File
}

var _ flow.Source = (*FileSource)(nil)

// NewSource creates a new file source.
func NewSource(l *slog.Logger, path string, opts ...flow.Option) (*FileSource, error) {
	// create common
	common := source.NewCommonSource(l, opts...)
	// open file
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	// create source
	fs := &FileSource{
		Common: common,
		logger: l,
		f:      f,
	}

	// add clean func
	common.AddCleanFunc(func() {
		l.Debug("source: close file")
		f.Close()
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	common.RegisterProcess(fs.process)

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
				fs.logger.Debug("source: end of file")
				break
			}
			fs.logger.Error(err.Error())
			continue
		}
		// send to channel
		fs.Send(line)
	}
}
