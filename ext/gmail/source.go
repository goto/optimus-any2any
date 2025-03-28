package gmail

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"

	extcommon "github.com/goto/optimus-any2any/ext/common"
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
	commonSource.SetName("source(gmail)")
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
		commonSource.Logger.Debug(fmt.Sprintf("%s: close gmail service", gs.Name()))
	})
	commonSource.RegisterProcess(gs.process)

	return gs, nil
}

// process reads data from Gmail.
func (gs *GmailSource) process() error {
	// get messages
	user := "me"
	r, err := gs.service.Users.Messages.List(user).Q(gs.filterRules).Do()
	if err != nil {
		gs.Logger.Error(fmt.Sprintf("%s: failed to list messages %s", gs.Name(), err.Error()))
		return errors.WithStack(err)
	}
	if len(r.Messages) == 0 {
		gs.Logger.Info(fmt.Sprintf("%s: no messages found", gs.Name()))
		return nil
	}

	// process messages
	for _, m := range r.Messages {
		msg, err := gs.service.Users.Messages.Get(user, m.Id).Do()
		if err != nil {
			gs.Logger.Error(fmt.Sprintf("%s: failed to get message %s", gs.Name(), err.Error()))
			return errors.WithStack(err)
		}
		gs.Logger.Info(fmt.Sprintf("%s: fetched message %s", gs.Name(), msg.Id))

		// extract data
		for _, p := range msg.Payload.Parts {
			if p.Filename == "" {
				gs.Logger.Debug(fmt.Sprintf("%s: no attachment found", gs.Name()))
				continue
			}
			// get attachment
			gs.Logger.Info(fmt.Sprintf("%s: found attachment %s", gs.Name(), p.Filename))
			attachment, err := gs.service.Users.Messages.Attachments.Get(user, msg.Id, p.Body.AttachmentId).Do()
			if err != nil {
				gs.Logger.Error(fmt.Sprintf("%s: failed to get attachment %s", gs.Name(), err.Error()))
				return errors.WithStack(err)
			}
			// decode attachment
			data, err := base64.URLEncoding.DecodeString(attachment.Data)
			if err != nil {
				gs.Logger.Error(fmt.Sprintf("%s: failed to decode attachment %s", gs.Name(), err.Error()))
				return errors.WithStack(err)
			}

			// convert to json
			var reader io.Reader
			switch filepath.Ext(p.Filename) {
			case ".json":
				reader = bytes.NewReader(data)
			case ".csv":
				reader = extcommon.FromCSVToJSON(gs.Logger, bytes.NewReader(data), false)
			case ".tsv":
				reader = extcommon.FromCSVToJSON(gs.Logger, bytes.NewReader(data), false, rune('\t'))
			default:
				gs.Logger.Warn(fmt.Sprintf("%s: unsupported file format: %s, use default (json)", gs.Name(), filepath.Ext(p.Filename)))
				reader = bytes.NewReader(data)
			}

			// send records
			sc := bufio.NewScanner(reader)
			for sc.Scan() {
				// read line
				raw := sc.Bytes()
				line := make([]byte, len(raw)) // Important: make a copy of the line before sending
				copy(line, raw)
				// send to channel
				gs.Send(line)
			}
		}
	}
	return nil
}
