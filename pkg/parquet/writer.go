package parquet

import (
	"context"
	"fmt"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/prometheus/prompb"
	log "github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/acrlabs/prom2parquet/pkg/backends"
)

const pageNum = 4

type Prom2ParquetWriter struct {
	backend backends.StorageBackend
	root    string
	prefix  string

	currentFile   string
	nextFlushTime time.Time
	pw            *writer.ParquetWriter

	clock clockwork.Clock
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
	root, prefix string,
	backend backends.StorageBackend,
) (*Prom2ParquetWriter, error) {
	return &Prom2ParquetWriter{
		backend: backend,
		root:    root,
		prefix:  prefix,

		clock: clockwork.NewRealClock(),
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

			dp := createDataPointForLabels(ts.Labels)
			for _, s := range ts.Samples {
				dp.Value = s.Value
				dp.Timestamp = s.Timestamp

				if err := self.pw.Write(dp); err != nil {
					log.Errorf("could not write datapoint: %v", err)
				}
			}
		case <-flushTicker.C:
			if time.Now().After(self.nextFlushTime) {
				log.Infof("Flush triggered: %v >= %v", time.Now(), self.nextFlushTime)
				if err := self.flush(); err != nil {
					log.Errorf("could not flush data: %v", err)
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
	}
}

func (self *Prom2ParquetWriter) flush() error {
	now := self.clock.Now().UTC()

	self.closeFile()

	self.currentFile = fmt.Sprintf("%s/%s.parquet", self.prefix, now.Format("2006010215"))
	fw, err := backends.ConstructBackendForFile(self.root, self.currentFile, self.backend)
	if err != nil {
		return fmt.Errorf("can't create storage backend writer: %w", err)
	}

	pw, err := writer.NewParquetWriter(fw, new(DataPoint), pageNum)
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
