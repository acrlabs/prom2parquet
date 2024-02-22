package parquet

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
)

const (
	podLabel       = "testing-12345-asdf"
	namespaceLabel = "the-namespace"
	nodeLabel      = "the-node"
	containerLabel = "the-container"
)

func TestCreateDataPointForLabels(t *testing.T) {
	labels := []prompb.Label{
		{
			Name:  model.MetricNameLabel,
			Value: "kube_node_stuff",
		},
		{
			Name:  podNameKey,
			Value: podLabel,
		},
		{
			Name:  "other-label",
			Value: "foo-bar",
		},
		{
			Name:  namespaceKey,
			Value: namespaceLabel,
		},
		{
			Name:  nodeKey,
			Value: nodeLabel,
		},
		{
			Name:  containerKey,
			Value: containerLabel,
		},
		{
			Name:  "a-label",
			Value: "baz-buz",
		},
	}

	dp := createDataPointForLabels(labels)
	assert.Equal(t, podLabel, dp.Pod)
	assert.Equal(t, containerLabel, dp.Container)
	assert.Equal(t, namespaceLabel, dp.Namespace)
	assert.Equal(t, nodeLabel, dp.Node)
	assert.Equal(t, "a-label=baz-buz,other-label=foo-bar", dp.Labels)
}
