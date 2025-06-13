package sftp

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/archive"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	xnet "github.com/goto/optimus-any2any/internal/net"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
)

// SFTPSink is a sink that writes data to a SFTP server.
type SFTPSink struct {
	common.Sink

	client                 *sftp.Client
	destinationURITemplate *template.Template
	writerHandlers         map[string]xio.WriteFlushCloser
	recordCounter          int

	// archive properties
	enableArchive       bool
	compressionType     string
	compressionPassword string

	// json path selector
	jsonPathSelector string
}

var _ flow.Sink = (*SFTPSink)(nil)

// NewSink creates a new SFTPSink.
func NewSink(commonSink common.Sink,
	privateKey, hostFingerprint string,
	destinationURI string,
	compressionType string, compressionPassword string,
	jsonPathSelector string,
	opts ...common.Option) (*SFTPSink, error) {

	// set up SFTP client
	urlParsed, err := url.Parse(destinationURI)
	if err != nil {
		err = fmt.Errorf("error parsing destination uri")
		return nil, errors.WithStack(err)
	}
	if urlParsed.Scheme != "sftp" {
		return nil, fmt.Errorf("invalid scheme: %s", urlParsed.Scheme)
	}
	address := urlParsed.Host
	username := urlParsed.User.Username()
	password, _ := urlParsed.User.Password()
	client, err := newClient(address, username, password, privateKey, hostFingerprint)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	u := url.URL{Scheme: urlParsed.Scheme, Path: urlParsed.Path}
	t, err := compiler.NewTemplate("sink_sftp_destination_uri", u.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}

	s := &SFTPSink{
		Sink:                   commonSink,
		client:                 client,
		destinationURITemplate: t,
		writerHandlers:         map[string]xio.WriteFlushCloser{},
		// archive options
		enableArchive:       compressionType != "",
		compressionType:     compressionType,
		compressionPassword: compressionPassword,
		// jsonPath selector
		jsonPathSelector: jsonPathSelector,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s.Logger().Info(fmt.Sprintf("close client"))
		return s.client.Close()
	})
	commonSink.AddCleanFunc(func() error {
		for _, fh := range s.writerHandlers {
			fh.Close()
		}
		s.Logger().Info("file handlers closed")
		return nil
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(s.process)

	return s, nil
}

func (s *SFTPSink) process() error {
	for record, err := range s.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		if s.IsSpecializedMetadataRecord(record) {
			s.Logger().Debug("skip specialized metadata record")
			continue
		}

		destinationURI, err := compiler.Compile(s.destinationURITemplate, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to compile destination URI"))
			return errors.WithStack(err)
		}
		s.Logger().Debug(fmt.Sprintf("destination URI: %s", destinationURI))
		wh, ok := s.writerHandlers[destinationURI]
		if !ok {
			var sfh io.Writer
			if s.enableArchive {
				tmpPath, err := getTmpPath(destinationURI)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to get tmp path for %s: %s", destinationURI, err.Error()))
					return errors.WithStack(err)
				}

				sfh, err = xio.NewWriteHandler(s.Logger(), tmpPath)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to create write handler: %s", err.Error()))
					return errors.WithStack(err)
				}
			} else {
				targetURI, err := url.Parse(destinationURI)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to parse destination URI"))
					return errors.WithStack(err)
				}
				if targetURI.Scheme != "sftp" {
					s.Logger().Error(fmt.Sprintf("invalid scheme"))
					return fmt.Errorf("invalid scheme: %s", targetURI.Scheme)
				}

				sfh, err = s.client.OpenFile(targetURI.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to create file handler: %s", err.Error()))
					return errors.WithStack(err)
				}
			}

			wh = xio.NewChunkWriter(
				s.Logger(), sfh,
				xio.WithExtension(filepath.Ext(destinationURI)),
			)
			s.writerHandlers[destinationURI] = wh
		}

		// record without metadata
		recordWithoutMetadata := s.RecordWithoutMetadata(record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}
		// if jsonPathSelector is provided, select the data using it
		if s.jsonPathSelector != "" {
			raw, err = s.JSONPathSelector(raw, s.jsonPathSelector)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to select data using json path selector"))
				return errors.WithStack(err)
			}
		}

		err = s.DryRunable(func() error {
			_, err = wh.Write(append(raw, '\n'))
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to write data"))
				return errors.WithStack(err)
			}

			s.recordCounter++
			return nil
		}, func() error {
			// in dry run mode, we don't need to send the request
			// we just need to check the endpoint connectivity
			targetURI, _ := url.Parse(destinationURI)
			return xnet.ConnCheck(targetURI.Host)
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// flush remaining records
	err := s.DryRunable(func() error {
		for destinationURI, fh := range s.writerHandlers {
			if err := fh.Flush(); err != nil {
				s.Logger().Error(fmt.Sprintf("failed to flush to %s", destinationURI))
				return errors.WithStack(err)
			}
			s.Logger().Info(fmt.Sprintf("flushed file: %s", destinationURI))
		}

		s.Logger().Info(fmt.Sprintf("successfully written %d records total to destination", s.recordCounter))
		return nil
	}, func() error {
		// in dry run mode, we don't need to send the request
		// we just need to check the endpoint connectivity
		for destinationURI := range s.writerHandlers {
			targetURI, _ := url.Parse(destinationURI)
			return xnet.ConnCheck(targetURI.Host)
		}
		return nil
	})
	if err != nil {
		s.Logger().Error(fmt.Sprintf("failed to flush data: %s", err.Error()))
		return errors.WithStack(err)
	}

	if s.enableArchive {
		err := s.DryRunable(func() error {
			pathsToArchive := []string{}
			for destinationURI := range s.writerHandlers {
				tmpPath, err := getTmpPath(destinationURI)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to get tmp path for %s: %s", destinationURI, err.Error()))
					return errors.WithStack(err)
				}

				pathsToArchive = append(pathsToArchive, tmpPath)
			}
			s.Logger().Info(fmt.Sprintf("compressing %d files: %s", len(pathsToArchive), strings.Join(pathsToArchive, ", ")))

			archivePaths, err := s.archive(pathsToArchive)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to compress files: %s", err.Error()))
				return errors.WithStack(err)
			}

			s.Logger().Info(fmt.Sprintf("successfully uploaded archive file(s) to SFTP: %s", strings.Join(archivePaths, ", ")))

			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func getTmpPath(destinationURI string) (string, error) {
	targetURI, err := url.Parse(destinationURI)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return filepath.Join("/tmp", filepath.Base(targetURI.Path)), nil
}

func (s *SFTPSink) archive(filesToArchive []string) ([]string, error) {
	templateURI := s.destinationURITemplate.Root.String()
	destinationDir := strings.TrimRight(strings.TrimSuffix(templateURI, filepath.Base(templateURI)), "/")

	var archiveDestinationPaths []string
	switch s.compressionType {
	case "gz", "gzip":
		for _, filePath := range filesToArchive {
			fileName := fmt.Sprintf("%s.%s", filepath.Base(filePath), s.compressionType)
			archiveDestinationPath := fmt.Sprintf("%s/%s", destinationDir, fileName)
			archiveDestinationPaths = append(archiveDestinationPaths, archiveDestinationPath)

			sftpArchive, err := s.client.OpenFile(archiveDestinationPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to create file handler: %s", err.Error()))
				return archiveDestinationPaths, errors.WithStack(err)
			}
			defer sftpArchive.Close()

			archiver := archive.NewFileArchiver(s.Logger(), archive.WithExtension(s.compressionType))
			if err := archiver.Archive([]string{filePath}, sftpArchive); err != nil {
				return archiveDestinationPaths, errors.WithStack(err)
			}
		}
	case "zip", "tar.gz":
		// for zip & tar.gz file, the whole file is archived into a single archive file
		// whose file name is deferred from the destination URI
		re := strings.NewReplacer("{{", "", "}}", "", "{{ ", "", " }}", "")
		fileName := fmt.Sprintf("%s.%s", strings.TrimSuffix(re.Replace(filepath.Base(templateURI)), filepath.Ext(templateURI)), s.compressionType)
		archiveDestinationPath := fmt.Sprintf("%s/%s", destinationDir, fileName)
		archiveDestinationPaths = append(archiveDestinationPaths, archiveDestinationPath)

		sftpArchive, err := s.client.OpenFile(archiveDestinationPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to create file handler: %s", err.Error()))
			return archiveDestinationPaths, errors.WithStack(err)
		}
		defer sftpArchive.Close()

		archiver := archive.NewFileArchiver(s.Logger(), archive.WithExtension(s.compressionType), archive.WithPassword(s.compressionPassword))
		if err := archiver.Archive(filesToArchive, sftpArchive); err != nil {
			return archiveDestinationPaths, errors.WithStack(err)
		}
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", s.compressionType)
	}

	return archiveDestinationPaths, nil
}
