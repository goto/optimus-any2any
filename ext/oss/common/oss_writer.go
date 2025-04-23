package common

import (
	"log/slog"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
)

type OSSWriter struct {
	client *oss.Client

	logger *slog.Logger
}
