package s3

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/goccy/go-json"
	xaws "github.com/goto/optimus-any2any/internal/auth/aws"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/internal/fs"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type S3Sink struct {
	common.Sink

	destinationURITemplate *template.Template
	handlers               fs.WriteHandler
	batchStepTemplate      *template.Template // TODO: deprecate this

	// json path selector
	jsonPathSelector string
}

var _ flow.Sink = (*S3Sink)(nil)

// NewS3Sink creates a new S3 sink instance
func NewSink(commonSink common.Sink, sinkCfg *config.SinkS3Config, opts ...common.Option) (*S3Sink, error) {

	// parse credentials
	creds, err := parseCredentials(sinkCfg.Credentials)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// get provider
	var provider aws.CredentialsProvider
	switch strings.ToLower(sinkCfg.Provider) {
	case string(xaws.TikTokProviderType):
		provider = xaws.NewTikTokProvider(creds.AWSAccessKeyID, creds.AWSSecretAccessKey, xaws.S3ResourceType)
	default:
		provider = credentials.NewStaticCredentialsProvider(creds.AWSAccessKeyID, creds.AWSSecretAccessKey, creds.AWSSessionToken)
	}

	// create S3 client uploader
	client, err := NewS3Client(commonSink.Context(), sinkCfg.Region, provider)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse destinationURI as template
	tmpl, err := compiler.NewTemplate("sink_s3_destination_uri", sinkCfg.DestinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}

	// prepare handlers
	handlers, err := NewS3Handler(commonSink.Context(), commonSink.Logger(),
		client, sinkCfg.EnableOverwrite,
		fs.WithConcurrentLimiter(commonSink),
		fs.WithWriteCompression(sinkCfg.CompressionType),
		fs.WithWriteCompressionPassword(sinkCfg.CompressionPassword),
		fs.WithWriteChunkOptions(
			xio.WithCSVSkipHeader(sinkCfg.SkipHeader),
			xio.WithCSVDelimiter(sinkCfg.CSVDelimiter),
		),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse batch step template // TODO: deprecate this, we keep this for backward compatibility
	var batchStepTmpl *template.Template
	if sinkCfg.BatchSize > 0 {
		batchStepTmpl, err = compiler.NewTemplate("sink_oss_batch_step", fmt.Sprintf("[[ mul (div .__METADATA__record_index %d) %d ]]", sinkCfg.BatchSize, sinkCfg.BatchSize))
		if err != nil {
			return nil, fmt.Errorf("failed to parse batch step template: %w", err)
		}
	}

	s3 := &S3Sink{
		Sink:                   commonSink,
		destinationURITemplate: tmpl,
		handlers:               handlers,
		batchStepTemplate:      batchStepTmpl,
		// jsonPath selector
		jsonPathSelector: sinkCfg.JSONPathSelector,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s3.Logger().Info("closing S3 files")
		s3.handlers.Close()
		return nil
	})

	// register sink process
	commonSink.RegisterProcess(s3.process)
	return s3, nil
}

func (s3 *S3Sink) process() error {
	recordCounter := 0

	for record, err := range s3.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		if s3.IsSpecializedMetadataRecord(record) {
			s3.Logger().Debug("skip specialized metadata record")
			continue
		}

		destinationURI, err := compiler.Compile(s3.destinationURITemplate, model.ToMap(record))
		if err != nil {
			s3.Logger().Error(fmt.Sprintf("failed to compile destination URI"))
			return errors.WithStack(err)
		}

		// TODO: deprecate this, we keep this for backward compatibility
		if s3.batchStepTemplate != nil {
			l, r := fs.SplitExtension(destinationURI)
			batchStep, err := compiler.Compile(s3.batchStepTemplate, model.ToMap(record))
			if err != nil {
				return errors.WithStack(err)
			}
			destinationURI = fmt.Sprintf("%s.%s", strings.TrimRight(destinationURI, l+r), batchStep) + l + r
		}

		// record without metadata
		recordWithoutMetadata := s3.RecordWithoutMetadata(record)
		raw, err := json.MarshalWithOption(recordWithoutMetadata, json.DisableHTMLEscape())
		if err != nil {
			s3.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}
		// if jsonPathSelector is provided, select the data using it
		if s3.jsonPathSelector != "" {
			raw, err = s3.JSONPathSelector(raw, s3.jsonPathSelector)
			if err != nil {
				s3.Logger().Error(fmt.Sprintf("failed to select data using json path selector"))
				return errors.WithStack(err)
			}
		}

		err = s3.DryRunable(func() error {
			if err := s3.handlers.Write(destinationURI, append(raw, '\n')); err != nil {
				s3.Logger().Error("failed to write to file")
				return errors.WithStack(err)
			}
			recordCounter++
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	if recordCounter == 0 {
		s3.Logger().Info(fmt.Sprintf("no records to write"))
		return nil
	}

	// flush all write handlers
	if err := s3.DryRunable(s3.handlers.Sync); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
