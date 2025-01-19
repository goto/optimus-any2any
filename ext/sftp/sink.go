package sftp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/component/sink"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
)

// SFTPSink is a sink that writes data to a SFTP server.
type SFTPSink struct {
	*sink.CommonSink
	client          *sftp.Client
	path            string
	filenamePattern string
	columnMap       map[string]string
	// batch size for the sink
	isGroupByBatch bool
	batchSize      int
	// group by column for the sink
	isGroupByColumn bool
	groupByColumn   string
}

var _ flow.Sink = (*SFTPSink)(nil)

// NewSink creates a new SFTPSink.
func NewSink(ctx context.Context, l *slog.Logger,
	address, username, password, privateKey, hostFingerprint string,
	path string,
	groupBy string, groupBatchSize int, groupColumnName string,
	columnMappingFilePath string,
	filenamePattern string,
	opts ...option.Option) (*SFTPSink, error) {
	// create common
	commonSink := sink.NewCommonSink(l, opts...)

	// set up SFTP client
	client, err := newClient(address, username, password, privateKey, hostFingerprint)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// create the directory if it does not exist
	if err := client.MkdirAll(path); err != nil {
		return nil, errors.WithStack(err)
	}

	// read column map
	columnMap, err := getColumnMap(columnMappingFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	s := &SFTPSink{
		CommonSink:      commonSink,
		client:          client,
		path:            path,
		filenamePattern: filenamePattern,
		columnMap:       columnMap,
		isGroupByBatch:  strings.ToLower(groupBy) == "batch",
		batchSize:       groupBatchSize,
		isGroupByColumn: strings.ToLower(groupBy) == "column",
		groupByColumn:   groupColumnName,
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Debug("sink(sftp): close func called")
		_ = client.Close()
		commonSink.Logger.Info("sink(sftp): client closed")
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(s.process)

	return s, nil
}

func (s *SFTPSink) process() {
	records := [][]byte{}
	values := map[string]string{}
	batchCount := 1

	for msg := range s.Read() {
		if s.Err() != nil {
			continue
		}
		b, ok := msg.([]byte)
		if !ok {
			s.Logger.Error(fmt.Sprintf("sink(sftp): message type assertion error: %T", msg))
			s.SetError(errors.New(fmt.Sprintf("sink(sftp): message type assertion error: %T", msg)))
			continue
		}
		s.Logger.Debug(fmt.Sprintf("sink(sftp): receive message: %s", string(b)))

		// modify the message based on the column map
		var val map[string]interface{}
		if err := json.Unmarshal(b, &val); err != nil {
			s.Logger.Error(fmt.Sprintf("sink(sftp): failed to unmarshal message: %s", err.Error()))
			s.SetError(errors.WithStack(err))
			continue
		}
		val = s.mapping(val)
		raw, err := json.Marshal(val)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("sink(sftp): failed to marshal message: %s", err.Error()))
			s.SetError(errors.WithStack(err))
			continue
		}

		records = append(records, raw)
		if s.isGroupByBatch && len(records) >= s.batchSize {
			s.Logger.Info(fmt.Sprintf("sink(sftp): (batch %d) uploading %d records", batchCount, len(records)))
			values["batch_start"] = fmt.Sprintf("%d", batchCount*int(s.batchSize))
			values["batch_end"] = fmt.Sprintf("%d", (batchCount+1)*int(s.batchSize))
			filename := renderFilename(s.filenamePattern, values)
			if err := s.upload(records, filename); err != nil {
				s.Logger.Error(fmt.Sprintf("sink(sftp): failed to upload batch %d: %s", batchCount, err.Error()))
				s.SetError(errors.WithStack(err))
			}
			batchCount++
			records = [][]byte{}

		}
	}

	// upload the remaining records
	if s.isGroupByBatch && len(records) > 0 {
		s.Logger.Info(fmt.Sprintf("sink(sftp): (batch %d) uploading %d records", batchCount, len(records)))
		values["batch_start"] = fmt.Sprintf("%d", batchCount*int(s.batchSize))
		values["batch_end"] = fmt.Sprintf("%d", (batchCount+1)*int(s.batchSize))
		filename := renderFilename(s.filenamePattern, values)
		if err := s.upload(records, filename); err != nil {
			s.Logger.Error(fmt.Sprintf("sink(sftp): failed to upload batch %d: %s", batchCount, err.Error()))
			s.SetError(errors.WithStack(err))
		}
		return
	}

	if s.isGroupByColumn {
		if len(records) > 0 {
			s.Logger.Info(fmt.Sprintf("sink(sftp): uploading %d records", len(records)))
			groupedRecords, err := s.groupRecordsByColumn(records)
			if err != nil {
				s.Logger.Error(fmt.Sprintf("sink(sftp): failed to group records by column: %s", err.Error()))
				s.SetError(errors.WithStack(err))
				return
			}
			for groupKey, groupRecords := range groupedRecords {
				s.Logger.Info(fmt.Sprintf("sink(sftp): uploading %d records for group %s", len(groupRecords), groupKey))
				values[s.groupByColumn] = groupKey
				filename := renderFilename(s.filenamePattern, values)
				if err := s.upload(groupRecords, filename); err != nil {
					s.Logger.Error(fmt.Sprintf("sink(sftp): failed to upload records for group %s: %s", groupKey, err.Error()))
					s.SetError(errors.WithStack(err))
				}
			}
		}
		return
	}

	// upload all records if not grouped
	if len(records) > 0 {
		s.Logger.Info(fmt.Sprintf("sink(sftp): uploading %d records", len(records)))
		filename := renderFilename(s.filenamePattern, values)
		if err := s.upload(records, filename); err != nil {
			s.Logger.Error(fmt.Sprintf("sink(sftp): failed to upload records: %s", err.Error()))
			s.SetError(errors.WithStack(err))
		}
	}
}

func (s *SFTPSink) mapping(value map[string]interface{}) map[string]interface{} {
	if s.columnMap == nil {
		return value
	}
	s.Logger.Debug(fmt.Sprintf("sink(sftp): record before map: %v", value))
	mappedValue := make(map[string]interface{})
	for key, val := range value {
		if mappedKey, ok := s.columnMap[key]; ok {
			mappedValue[mappedKey] = val
		} else {
			mappedValue[key] = val
		}
	}
	s.Logger.Debug(fmt.Sprintf("sink(sftp): record after map: %v", mappedValue))
	return mappedValue
}

func (s *SFTPSink) upload(records [][]byte, filename string) error {
	s.Logger.Debug(fmt.Sprintf("sink(sftp): uploading %d records to %s", len(records), filename))
	file, err := s.client.Create(fmt.Sprintf("%s/%s", s.path, filename))
	if err != nil {
		return errors.WithStack(err)
	}
	defer file.Close()

	data := bytes.Join(records, []byte("\n"))
	_, err = file.Write(data)
	if err != nil {
		return errors.WithStack(err)
	}
	s.Logger.Debug(fmt.Sprintf("sink(sftp): uploaded %d records to %s", len(records), filename))
	return nil
}

func (s *SFTPSink) groupRecordsByColumn(records [][]byte) (map[string][][]byte, error) {
	s.Logger.Info(fmt.Sprintf("sink(sftp): grouping records by column: %s", s.groupByColumn))
	groupRecords := map[string][][]byte{}
	for _, raw := range records {
		// parse the record
		var v map[string]interface{}
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, errors.WithStack(err)
		}
		groupKeyRaw, ok := v[s.groupByColumn]
		if !ok {
			return nil, errors.New(fmt.Sprintf("group by column not found: %s", s.groupByColumn))
		}
		groupKey := fmt.Sprintf("%v", groupKeyRaw)
		groupRecords[groupKey] = append(groupRecords[groupKey], raw)
	}
	s.Logger.Info(fmt.Sprintf("sink(sftp): %d groups found", len(groupRecords)))
	return groupRecords, nil
}

func renderFilename(filenamePattern string, values map[string]string) string {
	// Replace the filename pattern with the values
	for k, v := range values {
		filenamePattern = strings.ReplaceAll(filenamePattern, fmt.Sprintf("{%s}", k), v)
	}
	return filenamePattern
}

// getColumnMap reads the column map from the file.
func getColumnMap(columnMapFilePath string) (map[string]string, error) {
	if columnMapFilePath == "" {
		return nil, nil
	}
	columnMapRaw, err := os.ReadFile(columnMapFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	columnMap := make(map[string]string)
	if err = json.Unmarshal(columnMapRaw, &columnMap); err != nil {
		return nil, errors.WithStack(err)
	}
	return columnMap, nil
}
