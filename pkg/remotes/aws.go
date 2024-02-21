package remotes

import (
	"context"
	"fmt"
	"os"
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

func NewAWSStore(ctx context.Context, bucket, rootPrefix string) (*AWSRemoteStore, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not read AWS config: %w", err)
	}

	return &AWSRemoteStore{
		bucket:     bucket,
		rootPrefix: rootPrefix,
		s3Client:   s3.NewFromConfig(cfg),
	}, nil
}

func (self *AWSRemoteStore) Save(currentFile string) error {
	file, err := os.Open(currentFile)
	if err != nil {
		return fmt.Errorf("could not open %s: %w", currentFile, err)
	}
	defer file.Close()

	key := strings.TrimPrefix(currentFile, self.rootPrefix+"/")
	log.Infof("saving a copy of %s to s3://%s/%s", currentFile, self.bucket, key)
	if _, err := self.s3Client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: &self.bucket,
		Key:    &key,
		Body:   file,
	}); err != nil {
		return fmt.Errorf("could not write %s to S3: %w", currentFile, err)
	}

	return nil
}
