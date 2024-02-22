package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/acrlabs/prom2parquet/pkg/backends"
)

const metricName = "kube_node_stuff"

// These tests are actually super-hacky and brittle, but I'm not too sure how to do these tests a different way.  It
// would be nice to not just be comparing log outputs to see which code paths were taken, for one thing...
//
// Also, these tests are sortof inherently race-y, as evidenced by this delightful constant:
const sleepTime = 100 * time.Millisecond

func TestServerRun(t *testing.T) {
	cases := map[string]struct {
		operations          func(*promserver)
		expectedLogEntries  []string
		forbiddenLogEntries []string
	}{
		"kill": {
			operations: func(s *promserver) { close(s.killChannel) },
			expectedLogEntries: []string{
				"server failed",
				"flushing all data files",
				"shutting down",
			},
			forbiddenLogEntries: []string{
				"SIGUSR1 received",
				"recovered from panic",
			},
		},
		"flush": {
			operations: func(s *promserver) { close(s.flushChannel) },
			expectedLogEntries: []string{
				"server failed",
				"flushing all data files",
				"SIGUSR1 received",
			},
			forbiddenLogEntries: []string{
				"shutting down",
				"recovered from panic",
			},
		},
		"flush then kill": {
			operations: func(s *promserver) { close(s.flushChannel); time.Sleep(sleepTime); close(s.killChannel) },
			expectedLogEntries: []string{
				"server failed",
				"flushing all data files",
				"SIGUSR1 received",
				"shutting down",
				"recovered from panic",
			},
			forbiddenLogEntries: []string{},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			logs := test.NewGlobal()
			srv := newServer(&options{})
			srv.channels["foo"] = make(chan prompb.TimeSeries)
			go srv.run()

			tc.operations(srv)
			time.Sleep(sleepTime)

			entries := logs.AllEntries()

			for _, expected := range tc.expectedLogEntries {
				assert.GreaterOrEqual(t, len(lo.Filter(entries, func(e *log.Entry, _ int) bool {
					return strings.Contains(e.Message, expected)
				})), 1)
			}

			for _, forbidden := range tc.forbiddenLogEntries {
				assert.Len(t, lo.Filter(entries, func(e *log.Entry, _ int) bool {
					return strings.Contains(e.Message, forbidden)
				}), 0)
			}
		})
	}
}

func TestSendTimeseries(t *testing.T) {
	srv := newServer(&options{})
	srv.channels[metricName] = make(chan prompb.TimeSeries)

	ts := prompb.TimeSeries{
		Labels: []prompb.Label{
			{
				Name:  model.MetricNameLabel,
				Value: metricName,
			},
			{
				Name:  "foo",
				Value: "bar",
			},
			{
				Name:  "baz",
				Value: "buz",
			},
		},
		Samples: []prompb.Sample{
			{
				Value:     1.0,
				Timestamp: 0,
			},
			{
				Value:     2.0,
				Timestamp: 1,
			},
		},
	}

	go func() {
		err := srv.sendTimeseries(context.TODO(), []prompb.TimeSeries{ts})
		assert.Nil(t, err)
	}()

	val := <-srv.channels[metricName]
	assert.Equal(t, ts, val)
}

func TestSpawnWriter(t *testing.T) {
	srv := newServer(&options{backend: backends.Memory})
	_, err := srv.spawnWriter(context.TODO(), metricName)
	assert.Nil(t, err)
	assert.Contains(t, srv.channels, metricName)
}
