package file

import (
	"bufio"
	"context"
	"io"
	"os"

	"github.com/goto/optimus-any2any/pkg/flow"
)

type FileSource struct {
	ctx  context.Context
	path string
	c    chan any
}

var _ flow.Source = (*FileSource)(nil)

func NewSource(ctx context.Context, path string, opts ...flow.Option) *FileSource {
	fs := &FileSource{
		ctx:  ctx,
		path: path,
		c:    make(chan any),
	}
	for _, opt := range opts {
		opt(fs)
	}
	go fs.init()
	return fs
}

func (fs *FileSource) init() {
	defer func() {
		println("DEBUG: source: close")
		close(fs.c)
	}()
	// open file
	f, err := os.Open(fs.path)
	if err != nil {
		println(err.Error())
		return
	}
	defer func() {
		f.Close()
	}()

	// read file
	r := bufio.NewReader(f)
	for {
		// read line
		line, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			println(err.Error())
			continue
		}
		// send to channel
		println("DEBUG: source: send:", string(line))
		fs.c <- line
		println("DEBUG: source: done:", string(line))
	}
}

func (fs *FileSource) SetBufferSize(size int) {
	fs.c = make(chan any, size)
}

func (fs *FileSource) Out() <-chan any {
	return fs.c
}
