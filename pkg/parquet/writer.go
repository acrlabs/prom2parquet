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
	backend       backends.StorageBackend
	root          string
	prefix        string
	flushInterval time.Duration

	currentFile string
	pw          *writer.ParquetWriter

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
	flushInterval time.Duration,
) (*Prom2ParquetWriter, error) {
	return &Prom2ParquetWriter{
		backend:       backend,
		root:          root,
		prefix:        prefix,
		flushInterval: flushInterval,

		clock: clockwork.NewRealClock(),
	}, nil
}

func (self *Prom2ParquetWriter) Listen(stream <-chan prompb.TimeSeries) {
	self.listen(stream, self.getFlushTimer(), nil)
}

func (self *Prom2ParquetWriter) listen(
	stream <-chan prompb.TimeSeries,
	flushTimer <-chan time.Time,
	running chan<- bool, // used for testing
) {
	if err := self.createBackendWriter(); err != nil {
		log.Errorf("could not create backend writer: %v", err)
		return
	}

	// self.pw is a pointer to the writer instance, but it can get switched
	// out from under us whenever we flush; go defer evaluates the function
	// args when the defer call happens, not when the deferred function actually
	// executes, so here we need to use a double pointer so that we can make
	// sure we're closing the actual correct writer instance
	defer func(pw **writer.ParquetWriter) {
		closeFile(*pw)
		close(running)
	}(&self.pw)

	if running != nil {
		running <- true
	}

	for {
		select {
		case ts, ok := <-stream:
			if !ok {
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
		case <-flushTimer:
			flushTimer = self.getFlushTimer()
			log.Infof("flush triggered for %v", self.currentFile)

			// Run this in a separate goroutine so that writing the data
			// to S3 (with throttling or whatever) doesn't block the new incoming
			// datapoints
			go closeFile(self.pw)
			if err := self.createBackendWriter(); err != nil {
				log.Errorf("could not create backend writer: %v", err)
				return
			}
		}
	}
}

func (self *Prom2ParquetWriter) createBackendWriter() error {
	basename := self.now().Truncate(self.flushInterval).Format("20060102150405")
	self.currentFile = fmt.Sprintf("%s/%s.parquet", self.prefix, basename)

	fw, err := backends.ConstructBackendForFile(self.root, self.currentFile, self.backend)
	if err != nil {
		return fmt.Errorf("can't create storage backend: %w", err)
	}

	pw, err := writer.NewParquetWriter(fw, new(DataPoint), pageNum)
	if err != nil {
		return fmt.Errorf("can't create parquet writer: %w", err)
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	self.pw = pw

	return nil
}

func (self *Prom2ParquetWriter) getFlushTimer() <-chan time.Time {
	now := self.now()
	nextFlushTime := now.Truncate(self.flushInterval).Add(self.flushInterval)
	return time.After(nextFlushTime.Sub(now))
}

func (self *Prom2ParquetWriter) now() time.Time {
	return self.clock.Now().UTC()
}

func closeFile(pw *writer.ParquetWriter) {
	if pw != nil {
		if err := pw.WriteStop(); err != nil {
			log.Errorf("can't close parquet writer: %v", err)
		}
	}
}
