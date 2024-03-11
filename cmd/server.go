package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
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

const (
	prefixLabelKey = "prom2parquet_prefix"

	shutdownTime = 30 * time.Second
)

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
	mux.HandleFunc("/flush", s.flushData)

	return s
}

func (self *promserver) run() {
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

	log.Infof("server listening on %s", self.httpserv.Addr)
	<-endChannel
}

func (self *promserver) handleShutdown() {
	log.Info("shutting down...")
	log.Infof("flushing all data files")
	for _, ch := range self.channels {
		close(ch)
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), shutdownTime)
	defer cancel()
	if err := self.httpserv.Shutdown(ctxTimeout); err != nil {
		log.Errorf("failed shutting server down: %v", err)
	}

	timer := time.AfterFunc(shutdownTime, func() {
		os.Exit(0)
	})

	<-timer.C
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
		prefixLabel, _ := lo.Find(ts.Labels, func(i prompb.Label) bool { return i.Name == prefixLabelKey })
		channelName := prefixLabel.Value + "/" + nameLabel.Value

		log.Debugf("received timeseries data for %s", channelName)

		self.m.RLock()
		ch, ok = self.channels[channelName]
		self.m.RUnlock()

		if !ok {
			ch, err = self.spawnWriter(ctx, channelName)
			if err != nil {
				return fmt.Errorf("could not spawn timeseries writer for %s: %w", channelName, err)
			}
		}

		ch <- ts
	}

	return nil
}

func (self *promserver) spawnWriter(ctx context.Context, channelName string) (chan prompb.TimeSeries, error) {
	self.m.Lock()
	defer self.m.Unlock()

	log.Infof("new metric name seen, creating writer %s", channelName)
	writer, err := parquet.NewProm2ParquetWriter(
		ctx,
		self.opts.backendRoot,
		channelName,
		self.opts.backend,
	)
	if err != nil {
		return nil, fmt.Errorf("could not create writer for %s: %w", channelName, err)
	}
	ch := make(chan prompb.TimeSeries)
	self.channels[channelName] = ch

	go writer.Listen(ch) //nolint:contextcheck // the req context and the backend creation context should be separate

	return ch, nil
}

func (self *promserver) flushData(w http.ResponseWriter, req *http.Request) {
	d := json.NewDecoder(req.Body)
	d.DisallowUnknownFields()

	flushReq := struct {
		Prefix *string `json:"prefix"`
	}{}

	if err := d.Decode(&flushReq); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if flushReq.Prefix == nil {
		http.Error(w, "missing field 'prefix' in JSON object", http.StatusBadRequest)
		return
	}

	log.Infof("flushing all data for %s", *flushReq.Prefix)
	for chName, ch := range self.channels {
		if strings.HasPrefix(chName, *flushReq.Prefix) {
			close(ch)
		}
	}
}
