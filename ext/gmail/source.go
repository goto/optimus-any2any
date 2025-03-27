package gmail

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/ext/common/model"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"google.golang.org/api/gmail/v1"
)

// GmailSource is a source that reads data from Gmail.
type GmailSource struct {
	*common.Source
	service *gmail.Service

	filterRules string

	filenameColumn string
}

var _ flow.Source = (*GmailSource)(nil)

func NewSource(ctx context.Context, l *slog.Logger,
	tokenJSON string,
	filterRules, filenameColumn string,
	opts ...common.Option) (*GmailSource, error) {

	// create commonSource
	commonSource := common.NewSource(l, opts...)
	// create gmail service
	srv, err := NewServiceFromToken(ctx, []byte(tokenJSON))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create source
	gs := &GmailSource{
		Source:         commonSource,
		service:        srv,
		filterRules:    filterRules,
		filenameColumn: filenameColumn,
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
		records := make([]model.Record, 0)

		for _, p := range msg.Payload.Parts {
			if p.Filename == "" {
				gs.Logger.Debug("source(gmail): no attachment found")
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
			currentRecords, err := gs.convertToRecords(filepath.Ext(p.Filename)[1:], bytes.NewReader(data))
			if err != nil {
				gs.Logger.Error(fmt.Sprintf("source(gmail): failed to convert attachment to records %s", err.Error()))
				gs.SetError(errors.WithStack(err))
				return
			}
			// set filename column
			for i := range records {
				currentRecords[i].Set(gs.filenameColumn, p.Filename)
			}
			records = append(records, currentRecords...)
		}

		// map columns and send records
		for _, record := range records {
			raw, err := json.Marshal(record)
			if err != nil {
				gs.Logger.Error(fmt.Sprintf("source(gmail): failed to marshal record: %s", err.Error()))
				gs.SetError(errors.WithStack(err))
				continue
			}
			gs.Send(raw)
		}
	}
}

func (gs *GmailSource) convertToRecords(fileExt string, r io.Reader) ([]model.Record, error) {
	records := make([]model.Record, 0)
	switch fileExt {
	case "json":
		return extcommon.FromJSONToRecords(gs.Logger, r)
	case "csv":
		return extcommon.FromCSVToRecords(gs.Logger, r)
	default:
		return records, errors.New(fmt.Sprintf("source(gmail): unknown extractor file format: %s", fileExt))
	}
}
