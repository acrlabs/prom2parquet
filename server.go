package main

import (
	"net/http"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"

	"github.com/acrlabs/prom2parquet/pkg/parquet"
)

func main() {
	channels := map[string]chan prompb.TimeSeries{}

	defer func() {
		for _, ch := range channels {
			close(ch)
		}
	}()

	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		req, err := remote.DecodeWriteRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for _, ts := range req.Timeseries {
			name_label, _ := lo.Find(ts.Labels, func(i prompb.Label) bool { return i.Name == model.MetricNameLabel })
			metric_name := name_label.Value

			log.Infof("received timeseries data for %s", metric_name)
			if _, ok := channels[metric_name]; !ok {
				log.Infof("new metric name seen, creating writer %s", metric_name)
				writer := parquet.NewProm2ParquetWriter("/data", metric_name)
				ch := make(chan prompb.TimeSeries)
				channels[metric_name] = ch

				go writer.Listen(ch)
			}

			channels[metric_name] <- ts
		}
	})

	if err := http.ListenAndServe(":1234", nil); err != nil {
		log.Errorf("http server failed: %v", err)
	}
}
