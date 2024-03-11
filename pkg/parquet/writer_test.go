package parquet

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/mem"

	"github.com/acrlabs/prom2parquet/pkg/backends"
)

func TestFlush(t *testing.T) {
	fs := afero.NewMemMapFs()
	mem.SetInMemFileFs(&fs)

	w := Prom2ParquetWriter{
		backend: backends.Memory,
		root:    "/test",
		prefix:  "prefix/kube_node_stuff",

		clock: clockwork.NewFakeClockAt(time.Time{}),
	}

	err := w.flush()
	assert.Nil(t, err)

	exists, err := afero.Exists(fs, "/test/prefix/kube_node_stuff/0001010100.parquet")
	if err != nil {
		panic(err)
	}
	assert.True(t, exists)
}

func TestAdvanceFlushTime(t *testing.T) {
	w := Prom2ParquetWriter{}

	cases := map[string]struct {
		now      time.Time
		expected time.Time
	}{
		"same day": {
			now:      time.Date(2024, 02, 20, 21, 34, 45, 0, time.UTC),
			expected: time.Date(2024, 02, 20, 22, 0, 0, 0, time.UTC),
		},
		"next day": {
			now:      time.Date(2024, 02, 20, 23, 34, 45, 0, time.UTC),
			expected: time.Date(2024, 02, 21, 0, 0, 0, 0, time.UTC),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			w.advanceFlushTime(&tc.now)
			assert.Equal(t, tc.expected, w.nextFlushTime)
		})
	}
}
