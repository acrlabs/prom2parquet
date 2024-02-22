package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
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

	m            sync.RWMutex
	flushChannel chan os.Signal
	killChannel  chan os.Signal
}

func newServer(opts *options) *promserver {
	fulladdr := fmt.Sprintf(":%d", opts.port)
	mux := http.NewServeMux()

	s := &promserver{
		httpserv: &http.Server{Addr: fulladdr, Handler: mux, ReadHeaderTimeout: 10 * time.Second},
		opts:     opts,
		channels: map[string]chan prompb.TimeSeries{},

		flushChannel: make(chan os.Signal, 1),
		killChannel:  make(chan os.Signal, 1),
	}
	mux.HandleFunc("/receive", s.metricsReceive)

	return s
}

func (self *promserver) run() {
	signal.Notify(self.flushChannel, syscall.SIGUSR1)
	signal.Notify(self.killChannel, syscall.SIGTERM)

	endChannel := make(chan struct{}, 1)

	go func() {
		if err := self.httpserv.ListenAndServe(); err != nil {
			log.Errorf("server failed: %v", err)
		}
	}()

	go func() {
		<-self.killChannel
		self.handleShutdown()
		close(endChannel)
	}()

	go func() {
		<-self.flushChannel
		log.Infof("SIGUSR1 received; sleeping indefinitely")
		self.stopServer()
	}()

	log.Infof("server listening on %s", self.httpserv.Addr)
	<-endChannel
}

func (self *promserver) handleShutdown() {
	log.Info("shutting down...")
	defer func() {
		if r := recover(); r != nil {
			log.Warnf("recovered from panic, channels already closed")
		}
	}()

	self.stopServer()
	timer := time.AfterFunc(shutdownTime, func() {
		os.Exit(0)
	})

	<-timer.C
}

func (self *promserver) stopServer() {
	log.Infof("flushing all data files")
	for _, ch := range self.channels {
		close(ch)
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), shutdownTime)
	defer cancel()
	if err := self.httpserv.Shutdown(ctxTimeout); err != nil {
		log.Errorf("failed shutting server down: %v", err)
	}
}

func (self *promserver) metricsReceive(w http.ResponseWriter, req *http.Request) {
	body, err := remote.DecodeWriteRequest(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := self.sendTimeseries(req.Context(), body.Timeseries); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func (self *promserver) sendTimeseries(ctx context.Context, timeserieses []prompb.TimeSeries) (err error) {
	for _, ts := range timeserieses {
		// I'm not 100% sure which of these things would be recreated/shadowed below, so to be safe
		// I'm just declaring everything upfront
		var ch chan prompb.TimeSeries
		var ok bool

		nameLabel, _ := lo.Find(ts.Labels, func(i prompb.Label) bool { return i.Name == model.MetricNameLabel })
		metricName := nameLabel.Value

		log.Debugf("received timeseries data for %s", metricName)

		self.m.RLock()
		ch, ok = self.channels[metricName]
		self.m.RUnlock()

		if !ok {
			ch, err = self.spawnWriter(ctx, metricName)
			if err != nil {
				return fmt.Errorf("could not spawn timeseries writer for %s: %w", metricName, err)
			}
		}

		ch <- ts
	}

	return nil
}

func (self *promserver) spawnWriter(ctx context.Context, metricName string) (chan prompb.TimeSeries, error) {
	self.m.Lock()
	defer self.m.Unlock()

	log.Infof("new metric name seen, creating writer %s", metricName)
	writer, err := parquet.NewProm2ParquetWriter(
		ctx,
		self.opts.backendRoot,
		self.opts.prefix,
		metricName,
		self.opts.backend,
	)
	if err != nil {
		return nil, fmt.Errorf("could not create writer for %s: %w", metricName, err)
	}
	ch := make(chan prompb.TimeSeries)
	self.channels[metricName] = ch

	go writer.Listen(ch) //nolint:contextcheck // the req context and the backend creation context should be separate

	return ch, nil
}
