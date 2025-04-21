package s3

import (
	errs "errors"
	"io"
	"os"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	xaws "github.com/goto/optimus-any2any/internal/auth/aws"
	"github.com/goto/optimus-any2any/internal/component/common"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type S3Sink struct {
	common.Sink

	client                    *S3ClientUploader
	destinationURITemplate    *template.Template
	writeHandlers             map[string]xio.WriteFlusher // tmp write handler
	s3Handlers                map[string]io.WriteCloser
	fileRecordCounters        map[string]int
	batchSize                 int
	enableOverwrite           bool
	skipHeader                bool
	maxTempFileRecordNumber   int
	partialFileRecordCounters map[string]int // map of destinationURI to the number of records
}

var _ flow.Sink = (*S3Sink)(nil)

// NewS3Sink creates a new S3 sink instance
func NewS3Sink(commonSink common.Sink,
	key string, secret string, clientCredsProvider string,
	bucketName string, region string,
	destinationURITemplate *template.Template,
	batchSize int, enableOverwrite bool, skipHeader bool,
	maxTempFileRecordNumber int,
	opts ...common.Option) (*S3Sink, error) {

	// get provider
	var provider aws.CredentialsProvider
	switch clientCredsProvider {
	case string(xaws.TikTokProviderType):
		provider = xaws.NewTikTokProvider(key, secret, xaws.S3ResourceType)
	default:
		provider = credentials.NewStaticCredentialsProvider(key, secret, "")
	}

	// create S3 client uploader
	client, err := NewS3ClientUploader(commonSink.Context(), bucketName, region, provider)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	s3 := &S3Sink{
		Sink:                      commonSink,
		client:                    client,
		destinationURITemplate:    destinationURITemplate,
		writeHandlers:             make(map[string]xio.WriteFlusher),
		s3Handlers:                make(map[string]io.WriteCloser),
		fileRecordCounters:        make(map[string]int),
		batchSize:                 batchSize,
		enableOverwrite:           enableOverwrite,
		skipHeader:                skipHeader,
		maxTempFileRecordNumber:   maxTempFileRecordNumber,
		partialFileRecordCounters: make(map[string]int),
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s3.Logger().Info("closing S3 files")
		var e error
		for _, handler := range s3.s3Handlers {
			err := handler.Close()
			e = errs.Join(e, err)
		}
		return e
	})
	commonSink.AddCleanFunc(func() error {
		s3.Logger().Info("remove tmp files")
		var e error
		for tmpPath := range s3.writeHandlers {
			err := os.Remove(tmpPath)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			e = errs.Join(e, err)
		}
		return e
	})

	// register sink process
	commonSink.RegisterProcess(s3.process)
	return s3, nil
}

func (s3 *S3Sink) process() error {
	return nil
}
