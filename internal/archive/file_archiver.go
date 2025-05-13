package archive

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/yeka/zip"

	"github.com/pkg/errors"
)

type FileArchiver struct {
	l               *slog.Logger
	extension       string
	password        string
	archiveWriterFn ArchiveWriterFn
}

type ArchiveWriterFn func() (io.Writer, func() error, error)

func NewFileArchiver(l *slog.Logger, extension string, archiveWriterFn ArchiveWriterFn, opts ...FileArchiverOption) *FileArchiver {
	fa := &FileArchiver{
		l:               l,
		extension:       extension,
		archiveWriterFn: archiveWriterFn,
	}

	for _, opt := range opts {
		opt(fa)
	}
	return fa
}

func (f *FileArchiver) Archive(files []string) error {
	switch f.extension {
	case "tar.gz":
		return f.archiveTarGz(files)
	case "zip":
		return f.archiveZip(files)
	default:
		return fmt.Errorf("unsupported compression type: %s", f.extension)
	}
}

func (f *FileArchiver) archiveTarGz(files []string) error {
	destWriter, closeFn, err := f.archiveWriterFn()
	if err != nil {
		f.l.Debug(fmt.Sprintf("failed to create archive writer: %s", err.Error()))
		return errors.WithStack(err)
	}
	if closeFn != nil {
		defer closeFn()
	}

	gzWriter := gzip.NewWriter(destWriter)
	defer gzWriter.Close()

	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	for _, file := range files {
		err := f.addFileToTarWriter(file, tarWriter)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (f *FileArchiver) archiveZip(files []string) error {
	destWriter, closeFn, err := f.archiveWriterFn()
	if err != nil {
		f.l.Debug(fmt.Sprintf("failed to create archive writer: %s", err.Error()))
		return errors.WithStack(err)
	}
	if closeFn != nil {
		defer closeFn()
	}

	zipWriter := zip.NewWriter(destWriter)
	defer zipWriter.Close()

	for _, file := range files {
		err := f.addFileToZipWriter(file, zipWriter)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (f *FileArchiver) addFileToTarWriter(filePath string, tarWriter *tar.Writer) error {
	file, err := os.Open(filePath)
	if err != nil {
		f.l.Debug(fmt.Sprintf("failed to open file: %s", err.Error()))
		return errors.WithStack(err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		f.l.Debug(fmt.Sprintf("failed to get file info: %s", err.Error()))
		return errors.WithStack(err)
	}

	header := &tar.Header{
		Name: filepath.Base(filePath),
		Size: fileInfo.Size(),
		Mode: int64(fileInfo.Mode()),
	}

	if err := tarWriter.WriteHeader(header); err != nil {
		f.l.Debug(fmt.Sprintf("failed to write tar header: %s", err.Error()))
		return errors.WithStack(err)
	}

	n, err := io.Copy(tarWriter, file)
	if err != nil {
		f.l.Debug(fmt.Sprintf("failed to copy file to tar: %s", err.Error()))
		return errors.WithStack(err)
	}

	f.l.Debug(fmt.Sprintf("wrote %d bytes to tar.gz file", n))

	return nil
}

func (f *FileArchiver) addFileToZipWriter(filePath string, zipWriter *zip.Writer) error {
	file, err := os.Open(filePath)
	if err != nil {
		f.l.Debug(fmt.Sprintf("failed to open file: %s", err.Error()))
		return errors.WithStack(err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		f.l.Debug(fmt.Sprintf("failed to get file info: %s", err.Error()))
		return errors.WithStack(err)
	}

	header, err := zip.FileInfoHeader(fileInfo)
	if err != nil {
		f.l.Debug(fmt.Sprintf("failed to create zip header: %s", err.Error()))
		return errors.WithStack(err)
	}
	header.Name = filepath.Base(filePath)

	var writer io.Writer
	if f.password == "" {
		fh := &zip.FileHeader{
			Name:   header.Name,
			Method: zip.Deflate,
		}
		writer, err = zipWriter.CreateHeader(fh)
	} else {
		writer, err = zipWriter.Encrypt(filepath.Base(filePath), f.password, zip.AES256Encryption)
	}
	if err != nil {
		f.l.Debug(fmt.Sprintf("failed to create zip writer: %s", err.Error()))
		return errors.WithStack(err)
	}

	n, err := io.Copy(writer, file)
	if err != nil {
		f.l.Debug(fmt.Sprintf("failed to copy file to zip: %s", err.Error()))
		return errors.WithStack(err)
	}

	f.l.Debug(fmt.Sprintf("wrote %d bytes to zip file", n))

	return nil
}
