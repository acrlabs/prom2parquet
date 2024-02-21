package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"

	"github.com/acrlabs/prom2parquet/pkg/parquet"
)

const shutdownTime = 30 * time.Second

type promserver struct {
	httpserv *http.Server
	opts     *options
	channels map[string]chan prompb.TimeSeries
}

func newServer(opts *options) *promserver {
	fulladdr := fmt.Sprintf(":%d", opts.port)
	mux := http.NewServeMux()

	s := &promserver{
		httpserv: &http.Server{Addr: fulladdr, Handler: mux, ReadHeaderTimeout: 10 * time.Second},
		opts:     opts,
		channels: map[string]chan prompb.TimeSeries{},
	}
	mux.HandleFunc("/receive", s.metricsReceive)

	return s
}

func (self *promserver) handleShutdown() {
	log.Info("shutting down...")
	timer := time.AfterFunc(shutdownTime, func() {
		os.Exit(0)
	})
	defer timer.Stop()

	self.stopServer(false)

	<-timer.C
}

func (self *promserver) stopServer(stayAlive bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Warnf("recovered from panic, channels already closed")
		}
	}()

	log.Infof("flushing all data files")
	for _, ch := range self.channels {
		close(ch)
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), shutdownTime)
	defer cancel()
	if err := self.httpserv.Shutdown(ctxTimeout); err != nil {
		log.Errorf("failed shutting server down: %v", err)
	}

	if stayAlive {
		log.Infof("sleeping indefinitely")
		select {}
	}
}

func (self *promserver) run() {
	flushChannel := make(chan os.Signal, 1)
	signal.Notify(flushChannel, syscall.SIGUSR1)

	killChannel := make(chan os.Signal, 1)
	signal.Notify(killChannel, syscall.SIGTERM)

	endChannel := make(chan struct{}, 1)

	go func() {
		if err := self.httpserv.ListenAndServe(); err != nil {
			log.Errorf("server failed: %v", err)
		}
	}()

	go func() {
		<-killChannel
		self.handleShutdown()
		close(endChannel)
	}()

	go func() {
		<-flushChannel
		log.Infof("SIGUSR1 received")
		self.stopServer(true)
	}()

	log.Infof("server listening on %s", self.httpserv.Addr)
	<-endChannel
}

func (self *promserver) metricsReceive(w http.ResponseWriter, req *http.Request) {
	body, err := remote.DecodeWriteRequest(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, ts := range body.Timeseries {
		name_label, _ := lo.Find(ts.Labels, func(i prompb.Label) bool { return i.Name == model.MetricNameLabel })
		metric_name := name_label.Value

		log.Infof("received timeseries data for %s", metric_name)
		if _, ok := self.channels[metric_name]; !ok {
			log.Infof("new metric name seen, creating writer %s", metric_name)
			writer := parquet.NewProm2ParquetWriter(
				fmt.Sprintf("/data/%s", self.opts.prefix),
				metric_name,
				self.opts.cleanLocalStorage,
				self.opts.remote,
			)
			ch := make(chan prompb.TimeSeries)
			self.channels[metric_name] = ch

			go writer.Listen(ch)
		}

		self.channels[metric_name] <- ts
	}
}
