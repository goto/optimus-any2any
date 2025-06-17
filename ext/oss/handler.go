package oss

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/pkg/errors"
)

type ossHandler struct {
	*xio.CommonWriteHandler
	client *oss.Client
}

var _ xio.WriteHandler = (*ossHandler)(nil)

func NewOSSHandler(ctx context.Context, logger *slog.Logger, client *oss.Client, enableOverwrite bool, opts ...xio.Option) xio.WriteHandler {
	w := xio.NewCommonWriteHandler(ctx, logger, opts...)

	// prepare oss handler
	h := &ossHandler{
		CommonWriteHandler: w,
		client:             client,
	}

	// set appropriate schema and writer function
	h.SetSchema("oss")
	h.SetNewWriterFunc(func(destinationURI string) (io.Writer, error) {
		u, _ := url.Parse(destinationURI)
		if enableOverwrite {
			if err := h.remove(u.Host, u.Path); err != nil {
				return nil, errors.WithStack(err)
			}
		}
		// use inprogress suffix before flush it to actual destination
		path := strings.TrimLeft(u.Path, "/") + "_inprogress"
		oh, err := oss.NewAppendFile(h.Context(), h.client, u.Host, path)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return oh, nil
	})
	return h
}

func (h *ossHandler) remove(bucket, path string) error {
	var response *oss.DeleteObjectResult
	var err error
	// remove object
	response, err = h.client.DeleteObject(h.Context(), &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(path),
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if response.StatusCode >= 400 {
		err := errors.New(fmt.Sprintf("failed to delete object: %d", response.StatusCode))
		return errors.WithStack(err)
	}
	h.Logger().Debug(fmt.Sprintf("delete %s objects", path))
	return nil
}

func (h *ossHandler) Sync() error {
	h.CommonWriteHandler.Sync()
	for _, destinationURI := range h.DestinationURIs() {
		u, _ := url.Parse(destinationURI)
		result, err := h.client.CopyObject(h.Context(), &oss.CopyObjectRequest{
			SourceBucket: oss.Ptr(u.Host),
			SourceKey:    oss.Ptr(strings.TrimLeft(u.Path, "/") + "_inprogress"),
			Bucket:       oss.Ptr(u.Host),
			Key:          oss.Ptr(strings.TrimLeft(u.Path, "/")),
		})
		if err != nil {
			if result != nil {
				h.Logger().Error(fmt.Sprintf("rename object to final uri %s is failed with status %s and status code %d", destinationURI, result.Status, result.StatusCode))
			}
			return errors.WithStack(err)
		}
		h.Logger().Info(fmt.Sprintf("rename object to final uri %s is success with status %s", destinationURI, result.Status))
	}
	return nil
}
