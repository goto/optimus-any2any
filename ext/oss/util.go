// TODO: refactor this file to a common package
package oss

import (
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/pkg/errors"
)

func openFileURI(fileURI string) (io.ReadCloser, error) {
	u, err := url.Parse(fileURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return os.OpenFile(u.Path, os.O_RDONLY, 0644)
}

func toFileURI(ossURI string) string {
	return "file:///tmp/" + strings.TrimPrefix(ossURI, "oss://")
}

func toFilePaths(fileURIs []string) []string {
	paths := make([]string, len(fileURIs))
	for i, uri := range fileURIs {
		u, _ := url.Parse(uri)
		paths[i] = u.Path
	}
	return paths
}

func toFileURIs(filePaths []string) []string {
	uris := make([]string, len(filePaths))
	for i, path := range filePaths {
		uris[i] = "file://" + path
	}
	return uris
}

func toOSSURI(fileURI string) string {
	return "oss://" + strings.TrimPrefix(fileURI, "file:///tmp/")
}

func osscopy(client *Client, ossURI, fileURI string) error {
	// open local file
	src, err := openFileURI(fileURI)
	if err != nil {
		return errors.WithStack(err)
	}
	defer src.Close()
	// create oss writer
	dst, err := client.NewWriter(ossURI)
	if err != nil {
		return errors.WithStack(err)
	}
	defer dst.Close()

	// upload to OSS
	if _, err := io.Copy(dst, src); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
