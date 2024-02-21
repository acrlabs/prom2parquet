package parquet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/prometheus/prompb"
	log "github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/acrlabs/prom2parquet/pkg/remotes"
)

type Prom2ParquetWriter struct {
	rootPath string
	prefix   string
	metric   string

	currentFile       string
	cleanLocalStorage bool
	remote            remotes.Store
	nextFlushTime     time.Time
	pw                *writer.ParquetWriter
	clock             clockwork.Clock
}

type DataPoint struct {
	Timestamp int64   `parquet:"name=timestamp,type=INT64,convertedtype=TIMESTAMP"`
	Value     float64 `parquet:"name=value,type=DOUBLE"`

	Pod       string `parquet:"name=pod,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
	Container string `parquet:"name=container,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
	Namespace string `parquet:"name=namespace,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
	Node      string `parquet:"name=node,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
	Labels    string `parquet:"name=labels,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
}

func NewProm2ParquetWriter(
	ctx context.Context,
	rootPath, prefix, metric string,
	cleanLocalStorage bool,
	remote remotes.Endpoint,
) (*Prom2ParquetWriter, error) {
	var store remotes.Store
	var err error

	switch remote { //nolint:gocritic // eventually we'll support other backends
	case remotes.S3:
		store, err = remotes.NewAWSStore(ctx, "simkube", rootPath)
		if err != nil {
			return nil, fmt.Errorf("could not create AWS store: %w", err)
		}
	}

	return &Prom2ParquetWriter{
		rootPath: rootPath,
		prefix:   prefix,
		metric:   metric,

		cleanLocalStorage: cleanLocalStorage,
		remote:            store,
		clock:             clockwork.NewRealClock(),
	}, nil
}

func (self *Prom2ParquetWriter) Listen(stream <-chan prompb.TimeSeries) {
	if err := self.flush(); err != nil {
		log.Errorf("could not flush writer: %v", err)
		return
	}
	defer self.closeFile()

	flushTicker := time.NewTicker(time.Minute)

	for {
		select {
		case ts, ok := <-stream:
			if !ok {
				self.closeFile()
				return
			}

			dp := createDatapointForLabels(ts.Labels)
			for _, s := range ts.Samples {
				dp.Value = s.Value
				dp.Timestamp = s.Timestamp

				if err := self.pw.Write(dp); err != nil {
					log.Errorf("Could not write datapoint: %v", err)
				}
			}
		case <-flushTicker.C:
			if time.Now().After(self.nextFlushTime) {
				log.Infof("Flush triggered: %v >= %v", time.Now(), self.nextFlushTime)
				if err := self.flush(); err != nil {
					log.Errorf("Could not flush data: %v", err)
					return
				}
			}
		}
	}
}

func (self *Prom2ParquetWriter) closeFile() {
	if self.pw != nil {
		if err := self.pw.WriteStop(); err != nil {
			log.Errorf("can't close parquet writer: %v", err)
		}
		self.pw = nil

		go func() {
			if self.remote != nil {
				if err := self.remote.Save(self.currentFile); err != nil {
					log.Errorf("could not save %s to remote store: %v", self.currentFile, err)
				}
			}

			if self.cleanLocalStorage {
				if err := os.Remove(self.currentFile); err != nil {
					log.Errorf("could not remove local copy of %s: %v", self.currentFile, err)
				}
			}
		}()
	}
}

func (self *Prom2ParquetWriter) flush() error {
	self.closeFile()

	now := self.clock.Now().UTC()
	currentDir := fmt.Sprintf("%s/%s/%s", self.rootPath, self.prefix, self.metric)
	dirtyCurrentFile := fmt.Sprintf("%s/%s.parquet", currentDir, now.Format("2006010215"))
	currentFile, err := filepath.Abs(dirtyCurrentFile)
	if err != nil {
		return fmt.Errorf("couldn't compute absolute path for %s: %w", dirtyCurrentFile, err)
	}
	self.currentFile = currentFile

	log.Infof("writing metrics for %s to %s", self.metric, self.currentFile)

	if err := os.MkdirAll(currentDir, 0750); err != nil {
		return fmt.Errorf("can't create directory: %w", err)
	}

	fw, err := local.NewLocalFileWriter(self.currentFile)
	if err != nil {
		return fmt.Errorf("can't create filewriter: %w", err)
	}

	pw, err := writer.NewParquetWriter(fw, new(DataPoint), 4)
	if err != nil {
		return fmt.Errorf("can't create parquet writer: %w", err)
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	self.pw = pw
	self.advanceFlushTime(&now)
	return nil
}

func (self *Prom2ParquetWriter) advanceFlushTime(now *time.Time) {
	nextHour := now.Add(time.Hour)
	self.nextFlushTime = time.Date(
		nextHour.Year(),
		nextHour.Month(),
		nextHour.Day(),
		nextHour.Hour(),
		0,
		0,
		0,
		nextHour.Location(),
	)
}
