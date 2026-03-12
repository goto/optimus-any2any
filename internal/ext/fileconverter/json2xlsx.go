package fileconverter

import (
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/pkg/errors"
	"github.com/xuri/excelize/v2"
)

func JSON2XLSX(l *slog.Logger, src io.ReadSeeker, skipHeader bool) (*os.File, error) {
	// Create a temporary file to write the XLSX data
	dst, err := os.CreateTemp(os.TempDir(), "xlsx-*")
	if err != nil {
		l.Error(fmt.Sprintf("failed to open file: %v", err))
		return nil, errors.WithStack(err)
	}
	l.Debug(fmt.Sprintf("converting json to xlsx to tmp file: %s", dst.Name()))

	// reset the src file first
	if _, err = src.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}

	srcTransient, err := JSON2CSV(l, src, skipHeader, ',') // Convert JSON to CSV first
	if err != nil {
		l.Error(fmt.Sprintf("failed to convert json to csv: %v", err))
		return nil, errors.WithStack(err)
	}

	csvReader := csv.NewReader(srcTransient)
	csvReader.Comma = ','

	xlsxFile := excelize.NewFile()
	streamWriter, err := xlsxFile.NewStreamWriter("Sheet1")
	if err != nil {
		l.Error(fmt.Sprintf("failed to add sheet: %v", err))
		return nil, errors.WithStack(err)
	}

	rowIdx := 0
	for record, err := csvReader.Read(); err == nil; record, err = csvReader.Read() {
		cell, err := excelize.CoordinatesToCellName(1, rowIdx+1)
		if err != nil {
			l.Error(fmt.Sprintf("failed to convert coordinates to cell name: %v", err))
			return nil, errors.WithStack(err)
		}
		rowIdx++
		values := make([]interface{}, len(record))
		for i, field := range record {
			values[i] = field
		}
		if err := streamWriter.SetRow(cell, values); err != nil {
			l.Error(fmt.Sprintf("failed to set row: %v", err))
			return nil, errors.WithStack(err)
		}
	}

	if err := streamWriter.Flush(); err != nil {
		l.Error(fmt.Sprintf("failed to flush stream writer: %v", err))
		return nil, errors.WithStack(err)
	}

	if err := xlsxFile.Write(dst); err != nil {
		l.Error(fmt.Sprintf("failed to write xlsx file: %v", err))
		return nil, errors.WithStack(err)
	}

	if err := xlsxFile.Close(); err != nil {
		l.Error(fmt.Sprintf("failed to close xlsx file: %v", err))
		return nil, errors.WithStack(err)
	}

	if _, err := dst.Seek(0, io.SeekStart); err != nil {
		l.Error(fmt.Sprintf("failed to reset seek: %v", err))
		return nil, errors.WithStack(err)
	}

	l.Debug(fmt.Sprintf("successfully converted json to xlsx: %s", dst.Name()))
	return dst, nil
}
