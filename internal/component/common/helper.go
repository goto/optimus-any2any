package common

import (
	"fmt"
	"log/slog"
	"time"
)

func retry(l *slog.Logger, maxRetry int, f func() error) error {
	var err error
	sleepTime := int64(1)

	for i := 0; i < maxRetry; i++ {
		err = f()
		if err == nil {
			return nil
		}

		l.Warn(fmt.Sprintf("retry: %d, error: %v", i, err))
		sleepTime *= 1 << i
		time.Sleep(time.Duration(sleepTime) * time.Second)
	}

	return err
}
