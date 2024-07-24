package parquet

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/prometheus/prometheus/prompb"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/mem"

	"github.com/acrlabs/prom2parquet/pkg/backends"
)

func newTestProm2ParquetWriter(cl clockwork.Clock) *Prom2ParquetWriter {
	return &Prom2ParquetWriter{
		backend:       backends.Memory,
		root:          "/test",
		prefix:        "prefix/kube_node_stuff",
		flushInterval: 127 * time.Second,

		clock: cl,
	}
}

func TestListen(t *testing.T) {
	cases := map[string]struct {
		flush         bool
		expectedFiles []string
	}{
		"no flush": {
			expectedFiles: []string{
				"/test/prefix/kube_node_stuff/00010101000000.parquet",
			},
		},
		"flush": {
			flush: true,
			expectedFiles: []string{
				"/test/prefix/kube_node_stuff/00010101000000.parquet",
				"/test/prefix/kube_node_stuff/00010101000207.parquet",
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			mem.SetInMemFileFs(&fs)

			cl := clockwork.NewFakeClockAt(time.Time{})
			w := newTestProm2ParquetWriter(cl)
			stream := make(chan prompb.TimeSeries, 1)
			flushTimer := make(chan time.Time, 1)
			running := make(chan bool, 1)

			go w.listen(stream, flushTimer, running)

			// First block to make sure that all the setup is done (writer created, defer created)
			<-running

			if tc.flush {
				cl.Advance(w.flushInterval + time.Second)
				flushTimer <- w.clock.Now()
			}

			close(stream)

			// Next block until the end of the defer block to ensure the final flush is complete
			<-running

			for _, filename := range tc.expectedFiles {
				exists, err := afero.Exists(fs, filename)
				if err != nil {
					panic(err)
				}
				assert.True(t, exists)
			}
		})
	}
}

func TestCreateBackendWriter(t *testing.T) {
	w := newTestProm2ParquetWriter(clockwork.NewFakeClockAt(time.Date(2024, 3, 7, 10, 14, 30, 0, time.UTC)))
	err := w.createBackendWriter()
	assert.Nil(t, err)

	// time.Truncate(d) returns the result of rounding down to the nearest multiple of d since the zero time.
	// In this test, d = 127 seconds, and the zero time is always 0001-01-01T00:00:00Z.  The given test time
	// is 2024-03-07T10:14:30Z.  There are 63845403270 seconds between the zero time and the test time;
	// 63845403270 // 127 = 502719710, 502719710 * 127 = 63845403170, and 0001-01-01T00:00:00Z + 63845403170 seconds
	// is 2024-03-07T10:12:50Z.
	assert.Equal(t, w.currentFile, "prefix/kube_node_stuff/20240307101250.parquet")
	assert.NotNil(t, w.pw)
}
