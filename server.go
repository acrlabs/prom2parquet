package main

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type DataPoint struct {
	MetricName string  `parquet:"name=metric_name,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
	Labels     string  `parquet:"name=labels,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN"`
	Value      float64 `parquet:"name=value,type=DOUBLE"`
	Timestamp  int64   `parquet:"name=timestamp,type=INT64,convertedtype=TIMESTAMP"`
}

func main() {
	fw, err := local.NewLocalFileWriter("/data.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(DataPoint), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		req, err := remote.DecodeWriteRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for _, ts := range req.Timeseries {
			name, labels := "", []string{}
			for _, l := range ts.Labels {
				name = string(model.LabelName(l.Name))
				labels = append(labels, string(model.LabelValue(l.Value)))
			}

			label_str := strings.Join(labels, ",")
			for _, s := range ts.Samples {
				dp := DataPoint{
					MetricName: name,
					Labels:     label_str,
					Value:      s.Value,
					Timestamp:  s.Timestamp,
				}
				if err := pw.Write(dp); err != nil {
					log.Println("Could not write datapoint", err)
				}
			}
		}
	})

	go func() {
		time.Sleep(120 * time.Second)
		if err := pw.WriteStop(); err != nil {
			log.Println("WriteStop error", err)
		}
	}()

	log.Fatal(http.ListenAndServe(":1234", nil))
}
