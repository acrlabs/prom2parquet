package parquet

import (
	"fmt"
	"os"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/prometheus/prompb"
	log "github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type Prom2ParquetWriter struct {
	RootPath      string
	Metric        string
	nextFlushTime time.Time
	pw            *writer.ParquetWriter
	clock         clockwork.Clock
}

type DataPoint struct {
	Timestamp int64   `parquet:"name=timestamp,type=INT64,convertedtype=TIMESTAMP"`
	Value     float64 `parquet:"name=value,type=DOUBLE"`

	Pod       *string `parquet:"name=pod,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
	Container *string `parquet:"name=container,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
	Namespace *string `parquet:"name=namespace,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
	Node      *string `parquet:"name=node,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
	Labels    string  `parquet:"name=labels,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
}

func NewProm2ParquetWriter(rootPath, metric string) *Prom2ParquetWriter {
	return &Prom2ParquetWriter{RootPath: rootPath, Metric: metric, clock: clockwork.NewRealClock()}
}

func (self *Prom2ParquetWriter) Listen(stream <-chan prompb.TimeSeries) {
	self.flush()
	defer self.closeFile()

	flushTicker := time.NewTicker(time.Minute)

	for {
		select {
		case ts := <-stream:
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
			log.Errorf("Can't close parquet writer: %v", err)
		}
	}
}

func (self *Prom2ParquetWriter) flush() error {
	self.closeFile()

	now := self.clock.Now().UTC()
	currentDir := fmt.Sprintf("%s/%s", self.RootPath, now.Format("2006/01/02/15"))
	currentFile := fmt.Sprintf("%s/%s.parquet", currentDir, self.Metric)

	log.Infof("Writing metrics for %s to %s", self.Metric, currentFile)

	if err := os.MkdirAll(currentDir, 0750); err != nil {
		return fmt.Errorf("Can't create directory: %w", err)
	}

	fw, err := local.NewLocalFileWriter(currentFile)
	if err != nil {
		return fmt.Errorf("Can't create directory: %w", err)
	}

	pw, err := writer.NewParquetWriter(fw, new(DataPoint), 4)
	if err != nil {
		return fmt.Errorf("Can't create directory: %w", err)
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
