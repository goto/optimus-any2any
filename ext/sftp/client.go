package sftp

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func newClient(address, username, password, privateKey, hostFingerprint string) (*sftp.Client, error) {
	config := &ssh.ClientConfig{
		User:            username,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Minute,
		HostKeyAlgorithms: []string{
			ssh.KeyAlgoRSA,
		},
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
	return sftp.NewClient(conn)
}
