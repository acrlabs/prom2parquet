package remotes

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
)

type AWSRemoteStore struct {
	bucket     string
	rootPrefix string
	s3Client   *s3.Client
}

func NewAWSStore(bucket, rootPrefix string) (*AWSRemoteStore, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("could not read AWS config: %w", err)
	}

	return &AWSRemoteStore{
		bucket:     bucket,
		rootPrefix: rootPrefix,
		s3Client:   s3.NewFromConfig(cfg),
	}, nil
}

func (self *AWSRemoteStore) Save(root, currentDir string) error {
	currentPath := fmt.Sprintf("%s/%s", root, currentDir)

	if err := filepath.WalkDir(currentPath, func(path string, entry fs.DirEntry, err error) error {
		if !entry.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				log.Errorf("could not open file %s: %v", path, err)
			}
			defer file.Close()

			key := strings.TrimPrefix(path, root)
			if _, err := self.s3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: &self.bucket,
				Key:    &key,
				Body:   file,
			}); err != nil {
				log.Errorf("could not write file %s to S3: %v", path, err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("error saving contents to S3: %w", err)
	}

	return nil
}
