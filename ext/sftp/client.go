package sftp

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Client struct {
	*sftp.Client
}

func NewSFTPClientFromURI(destinationURI, privateKey, hostFingerprint string) (*Client, error) {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// create a new SFTP client
	password, _ := u.User.Password()
	return NewSFTPClient(u.Host, u.User.Username(), password, privateKey, hostFingerprint)
}

func NewSFTPClient(address, username, password, privateKey, hostFingerprint string) (*Client, error) {
	config := &ssh.ClientConfig{
		User:            username,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	if password != "" {
		config.Auth = append(config.Auth, ssh.Password(password))
	}
	if privateKey != "" {
		signer, err := ssh.ParsePrivateKey([]byte(privateKey))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		config.Auth = append(config.Auth, ssh.PublicKeys(signer))
	}
	if hostFingerprint != "" {
		config.HostKeyCallback = func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			hash := md5.Sum(key.Marshal())
			fingerprint := hex.EncodeToString(hash[:])
			// Compare the fingerprint with the known fingerprint
			if fingerprint != hostFingerprint {
				return fmt.Errorf("unknown host key fingerprint: %s", fingerprint)
			}
			return nil
		}
	}
	// connect to the server
	conn, err := ssh.Dial("tcp", address, config)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	c, err := sftp.NewClient(conn)
	if err != nil {
		conn.Close()
		return nil, errors.WithStack(err)
	}
	return &Client{c}, nil
}

// Close closes the SFTP client connection.
func (c *Client) Close() error {
	if c.Client == nil {
		return nil
	}
	err := c.Client.Close()
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (c *Client) NewWriter(destinationURI string) (io.WriteCloser, error) {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// create dir if it does not exist
	dir := filepath.Dir(u.Path)
	if err := c.MkdirAll(dir); err != nil {
		return nil, errors.WithStack(err)
	}
	// open or create file for writing
	return c.OpenFile(u.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
}

func (c *Client) Remove(destinationURI string) error {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return errors.WithStack(err)
	}
	// remove file
	if err := c.Remove(u.Path); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
