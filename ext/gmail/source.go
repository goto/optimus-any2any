package gmail

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/component/source"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"google.golang.org/api/gmail/v1"
)

// GmailSource is a source that reads data from Gmail.
type GmailSource struct {
	*source.CommonSource
	service *gmail.Service

	filterRules         string
	extractorSource     string
	extractorPattern    string
	extractorFileFormat string

	resultFilenameColumn string
	columnMap            map[string]string
}

var _ flow.Source = (*GmailSource)(nil)

func NewSource(ctx context.Context, l *slog.Logger,
	tokenJSON string,
	filterRules, extractorSource, extractorPattern, extractorFileFormat string,
	resultFilenameColumn, columnMappingFilePath string,
	opts ...option.Option) (*GmailSource, error) {

	// create commonSource
	commonSource := source.NewCommonSource(l, opts...)
	// create gmail service
	srv, err := NewServiceFromToken(ctx, []byte(tokenJSON))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// read column map
	columnMap, err := getColumnMap(columnMappingFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create source
	gs := &GmailSource{
		CommonSource:         commonSource,
		service:              srv,
		filterRules:          filterRules,
		extractorSource:      extractorSource,
		extractorPattern:     extractorPattern,
		extractorFileFormat:  extractorFileFormat,
		resultFilenameColumn: resultFilenameColumn,
		columnMap:            columnMap,
	}

	// add clean func
	commonSource.AddCleanFunc(func() {
		commonSource.Logger.Debug("source(gmail): close gmail service")
	})
	commonSource.RegisterProcess(gs.process)

	return gs, nil
}

// process reads data from Gmail.
func (gs *GmailSource) process() {
	// get messages
	user := "me"
	r, err := gs.service.Users.Messages.List(user).Q(gs.filterRules).Do()
	if err != nil {
		gs.Logger.Error(fmt.Sprintf("source(gmail): failed to list messages %s", err.Error()))
		gs.SetError(errors.WithStack(err))
		return
	}
	if len(r.Messages) == 0 {
		gs.Logger.Info("source(gmail): no messages found")
		return
	}

	// process messages
	for _, m := range r.Messages {
		msg, err := gs.service.Users.Messages.Get(user, m.Id).Do()
		if err != nil {
			gs.Logger.Error(fmt.Sprintf("source(gmail): failed to get message %s", err.Error()))
			gs.SetError(errors.WithStack(err))
			return
		}
		gs.Logger.Info(fmt.Sprintf("source(gmail): fetched message %s", msg.Id))

		// extract data
		records := make([]map[string]interface{}, 0)
		switch gs.extractorSource {
		case "attachment":
			for _, p := range msg.Payload.Parts {
				if p.Filename == "" {
					gs.Logger.Info("source(gmail): no attachment found")
					continue
				}
				// get attachment
				gs.Logger.Info(fmt.Sprintf("source(gmail): found attachment %s", p.Filename))
				attachment, err := gs.service.Users.Messages.Attachments.Get(user, msg.Id, p.Body.AttachmentId).Do()
				if err != nil {
					gs.Logger.Error(fmt.Sprintf("source(gmail): failed to get attachment %s", err.Error()))
					gs.SetError(errors.WithStack(err))
					return
				}
				// decode attachment
				data, err := base64.URLEncoding.DecodeString(attachment.Data)
				if err != nil {
					gs.Logger.Error(fmt.Sprintf("source(gmail): failed to decode attachment %s", err.Error()))
					gs.SetError(errors.WithStack(err))
					return
				}
				// convert to json
				currentRecords, err := gs.convertToRecords(data, p.Filename)
				if err != nil {
					gs.Logger.Error(fmt.Sprintf("source(gmail): failed to convert attachment to records %s", err.Error()))
					gs.SetError(errors.WithStack(err))
					return
				}
				records = append(records, currentRecords...)
			}
		case "body":
			// TODO: implement body extractor
			gs.Logger.Error("source(gmail): body extractor is not implemented")
			gs.SetError(errors.New("body extractor is not implemented"))
			return
		default:
			gs.Logger.Error(fmt.Sprintf("source(gmail): unknown extractor source %s", gs.extractorSource))
			gs.SetError(errors.New("unknown extractor source"))
			return
		}

		// map columns and send records
		for _, record := range records {
			mappedRecord := gs.mapping(record)
			raw, err := json.Marshal(mappedRecord)
			if err != nil {
				gs.Logger.Error(fmt.Sprintf("source(gmail): failed to marshal record: %s", err.Error()))
				gs.SetError(errors.WithStack(err))
				continue
			}
			gs.Send(raw)
		}
	}
}

func (gs *GmailSource) convertToRecords(data []byte, filename string) ([]map[string]interface{}, error) {
	records := make([]map[string]interface{}, 0)
	switch gs.extractorFileFormat {
	case "json":
		for _, recordRaw := range bytes.Split(data, []byte("\n")) {
			record := make(map[string]interface{})
			if err := json.Unmarshal(recordRaw, &record); err != nil {
				return nil, errors.WithStack(err)
			}
			records = append(records, record)
		}
	case "csv":
		r := csv.NewReader(strings.NewReader(string(data)))
		r.FieldsPerRecord = -1
		rows, err := r.ReadAll()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		// csv to json records
		for i, row := range rows {
			if i == 0 {
				continue
			}
			record := make(map[string]interface{})
			if len(row) != len(rows[0]) {
				gs.Logger.Warn(fmt.Sprintf("source(gmail): record %d has different column count", i))
				continue
			}
			for j, value := range row {
				record[rows[0][j]] = value
			}
			record[gs.resultFilenameColumn] = filename
			records = append(records, record)
		}
	}
	return records, nil
}

// mapping maps the column name from Gmail response to the column name in the column map.
func (gs *GmailSource) mapping(value map[string]interface{}) map[string]interface{} {
	if gs.columnMap == nil {
		return value
	}
	gs.Logger.Debug(fmt.Sprintf("source(gmail): record before map: %v", value))
	mappedValue := make(map[string]interface{})
	for key, val := range value {
		if mappedKey, ok := gs.columnMap[key]; ok {
			mappedValue[mappedKey] = val
		} else {
			mappedValue[key] = val
		}
	}
	gs.Logger.Debug(fmt.Sprintf("source(gmail): record after map: %v", mappedValue))
	return mappedValue
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
