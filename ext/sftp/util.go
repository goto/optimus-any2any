// TODO: refactor this file to a common package
package sftp

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

func toFileURI(sftpURI string) string {
	u, _ := url.Parse(sftpURI)
	return "file:///tmp/" + strings.TrimPrefix(u.Path, "/")
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

func toSFTPURI(username, password, fileURI string) string {
	u, _ := url.Parse(fileURI)
	u.User = url.UserPassword(username, password)
	u.Scheme = "sftp"
	u.Path = strings.TrimPrefix(u.Path, "/tmp/")
	return u.String()
}

func osscopy(client *Client, sftpURI, fileURI string) error {
	// open local file
	src, err := openFileURI(fileURI)
	if err != nil {
		return errors.WithStack(err)
	}
	defer src.Close()
	// create sftp writer
	dst, err := client.NewWriter(sftpURI)
	if err != nil {
		return errors.WithStack(err)
	}
	defer dst.Close()

	// upload to SFTP
	if _, err := io.Copy(dst, src); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
