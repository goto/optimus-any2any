package archive

import (
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/pkg/errors"
)

type MultiFileArchiver struct {
	l         *slog.Logger
	extension string
	mapper    FileToArchiveWriterMapper
}

type FileToArchiveWriterMapper func(filePath string) (io.Writer, func() error, error)

func NewMultiFileArchiver(l *slog.Logger, extension string, mapper FileToArchiveWriterMapper) *MultiFileArchiver {
	mfa := &MultiFileArchiver{
		l:         l,
		extension: extension,
		mapper:    mapper,
	}
	return mfa
}

func (mfa *MultiFileArchiver) Archive(files []string) error {
	return mfa.archiveGzip(files)
}

func (mfa *MultiFileArchiver) archiveGzip(files []string) error {
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			mfa.l.Error(fmt.Sprintf("failed to open file: %s", err.Error()))
			return errors.WithStack(err)
		}
		defer file.Close()

		writer, closeFn, err := mfa.mapper(filePath)
		if err != nil {
			mfa.l.Error(fmt.Sprintf("failed to create writer for file %s: %s", filePath, err.Error()))
			return errors.WithStack(err)
		}
		if closeFn != nil {
			defer closeFn()
		}

		gzWriter := gzip.NewWriter(writer)
		defer gzWriter.Close()

		if _, err := io.Copy(gzWriter, file); err != nil {
			mfa.l.Error(fmt.Sprintf("failed to write gz file: %s", err.Error()))
			return errors.WithStack(err)
		}
	}

	return nil
}
