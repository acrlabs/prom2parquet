package backends

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"github.com/thediveo/enumflag/v2"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/mem"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/source"
)

type StorageBackend enumflag.Flag

const (
	Local StorageBackend = iota
	Memory

	S3
)

func ConstructBackendForFile( //nolint:ireturn // this is fine
	root,
	file string,
	backend StorageBackend,
) (source.ParquetFile, error) {
	switch backend {
	case Local:
		fullPath, err := filepath.Abs(fmt.Sprintf("%s/%s", root, file))
		if err != nil {
			return nil, fmt.Errorf("can't construct local path %s/%s: %w", root, file, err)
		}

		log.Infof("using local backend, writing to %s", fullPath)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0750); err != nil {
			return nil, fmt.Errorf("can't create directory: %w", err)
		}

		fw, err := local.NewLocalFileWriter(fullPath)
		if err != nil {
			return nil, fmt.Errorf("can't create local file writer: %w", err)
		}

		return fw, nil

	case Memory:
		log.Warnf("using in-memory backend, this is intended for testing only!")

		fw, err := mem.NewMemFileWriter(file, nil)
		if err != nil {
			return nil, fmt.Errorf("can't create in-memory writer: %w", err)
		}

		return fw, nil

	case S3:
		log.Infof("using S3 backend, writing to s3://%s/%s", root, file)

		fw, err := s3v2.NewS3FileWriter(context.Background(), root, file, nil)
		if err != nil {
			return nil, fmt.Errorf("can't create S3 writer: %w", err)
		}

		return fw, nil
	}

	return nil, nil
}
