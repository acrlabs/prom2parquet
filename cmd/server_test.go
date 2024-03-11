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

const (
	metricName  = "kube_node_stuff"
	testPrefix  = "test-prefix"
	channelName = testPrefix + "/" + metricName
)

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
	srv.channels[channelName] = make(chan prompb.TimeSeries)

	ts := prompb.TimeSeries{
		Labels: []prompb.Label{
			{
				Name:  model.MetricNameLabel,
				Value: metricName,
			},
			{
				Name:  prefixLabelKey,
				Value: testPrefix,
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

	val := <-srv.channels[channelName]
	assert.Equal(t, ts, val)
}

func TestSpawnWriter(t *testing.T) {
	srv := newServer(&options{backend: backends.Memory})
	_, err := srv.spawnWriter(context.TODO(), channelName)
	assert.Nil(t, err)
	assert.Contains(t, srv.channels, channelName)
}
