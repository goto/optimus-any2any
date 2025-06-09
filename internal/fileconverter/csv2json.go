package fileconverter

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

func CSV2JSON(l *slog.Logger, src io.ReadSeeker, skipHeader bool, skipRows int, delimiter rune) (*os.File, error) {
	// Create a temporary file to write the JSON data
	dst, err := os.CreateTemp(os.TempDir(), "json-*")
	if err != nil {
		l.Error(fmt.Sprintf("failed to open file: %v", err))
		return nil, errors.WithStack(err)
	}
	l.Debug(fmt.Sprintf("converting csv to json to tmp file: %s", dst.Name()))

	// reset the src file first
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		l.Error(fmt.Sprintf("failed to reset seek: %v", err))
		return nil, errors.WithStack(err)
	}

	if skipRows > 0 {
		rowOffset := 0

		sc := bufio.NewScanner(src)
		buf := make([]byte, 0, 4*1024)
		sc.Buffer(buf, 1024*1024)

		for skipRows > 0 && sc.Scan() {
			raw := sc.Bytes()
			rowOffset += len(raw)
			skipRows--
		}
		if err := sc.Err(); err != nil {
			l.Error(fmt.Sprintf("failed to read csv: %v", err))
			return nil, errors.WithStack(err)
		}
		if _, err := src.Seek(int64(rowOffset), io.SeekStart); err != nil {
			l.Error(fmt.Sprintf("failed to reset seek: %v", err))
			return nil, errors.WithStack(err)
		}
	}

	csvReader := csv.NewReader(src)
	csvReader.Comma = delimiter

	headers := []string{}
	isHeader := true
	for record, err := csvReader.Read(); err == nil; record, err = csvReader.Read() {
		if isHeader {
			isHeader = false
			if !skipHeader {
				headers = record
				continue
			}
			for i := range record {
				headers = append(headers, fmt.Sprintf("%d", i))
			}
		}
		recordResult := model.NewRecord()
		for i, header := range headers {
			recordResult.Set(header, record[i])
		}
		raw, err := json.Marshal(recordResult)
		if err != nil {
			l.Error(fmt.Sprintf("failed to marshal json: %v", err))
			continue
		}
		if _, err := dst.Write(append(raw, '\n')); err != nil {
			l.Error(fmt.Sprintf("failed to write to pipe: %v", err))
			continue
		}
	}
	// ensure all data is written to the file
	dst.Sync()

	// reset dst file pointer to the beginning
	if _, err := dst.Seek(0, io.SeekStart); err != nil {
		l.Error(fmt.Sprintf("failed to reset seek: %v", err))
		return nil, errors.WithStack(err)
	}

	return dst, nil
}
