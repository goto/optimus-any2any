package redis

import (
	"context"
	"crypto/tls"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

func NewRedisClient(ctx context.Context, addresses []string, username, password string, dbNumber int, tlsConfig *tls.Config) (redis.Cmdable, error) {
	// single redis instance
	if len(addresses) == 1 {
		opt := &redis.Options{Addr: addresses[0], DB: dbNumber}
		if username != "" {
			opt.Username = username
		}
		if password != "" {
			opt.Password = password
		}
		if tlsConfig != nil {
			opt.TLSConfig = tlsConfig
		}

		client := redis.NewClient(opt)
		_, err := client.Ping(ctx).Result()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return client, nil
	}

	// cluster redis
	if len(addresses) > 1 {
		opt := &redis.ClusterOptions{Addrs: addresses}
		if username != "" {
			opt.Username = username
		}
		if password != "" {
			opt.Password = password
		}
		if tlsConfig != nil {
			opt.TLSConfig = tlsConfig
		}
		client := redis.NewClusterClient(opt)
		_, err := client.Ping(ctx).Result()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return client, nil
	}

	return nil, errors.New("no redis address provided")
}
