package parquet

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/common/model"
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
	Labels    string  `parquet:"name=labels,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
	Value     float64 `parquet:"name=value,type=DOUBLE"`
	Timestamp int64   `parquet:"name=timestamp,type=INT64,convertedtype=TIMESTAMP"`
}

func NewProm2ParquetWriter(rootPath, metric string) *Prom2ParquetWriter {
	return &Prom2ParquetWriter{RootPath: rootPath, Metric: metric, clock: clockwork.NewRealClock()}
}

func (self *Prom2ParquetWriter) Listen(stream <-chan prompb.TimeSeries) {
	for ts := range stream {
		if time.Now().After(self.nextFlushTime) {
			log.Infof("Current time is %v, which is after the next flush time of %v", time.Now(), self.nextFlushTime)
			if err := self.flush(); err != nil {
				log.Errorf("Could not flush data: %v", err)
				break
			}
		}

		labels := []string{}
		for _, l := range ts.Labels {
			labels = append(labels, string(model.LabelValue(l.Value)))
		}
		label_str := strings.Join(labels, ",")
		for _, s := range ts.Samples {
			dp := DataPoint{
				Labels:    label_str,
				Value:     s.Value,
				Timestamp: s.Timestamp,
			}
			if err := self.pw.Write(dp); err != nil {
				log.Errorf("Could not write datapoint: %v", err)
			}
		}
	}

	self.closeFile()
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
