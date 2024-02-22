package parquet

import (
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

const (
	podNameKey   = "pod"
	namespaceKey = "namespace"
	containerKey = "container"
	nodeKey      = "node"
)

func createDataPointForLabels(labels []prompb.Label) DataPoint {
	dp := DataPoint{}

	label_strs := []string{}
	for _, l := range labels {
		switch l.Name {
		case model.MetricNameLabel:
			continue
		case podNameKey:
			dp.Pod = l.Value
		case namespaceKey:
			dp.Namespace = l.Value
		case containerKey:
			dp.Container = l.Value
		case nodeKey:
			dp.Node = l.Value
		default:
			label_strs = append(label_strs, fmt.Sprintf("%s=%s", l.Name, l.Value))
		}
	}
	sort.Strings(label_strs)
	dp.Labels = strings.Join(label_strs, ",")

	return dp
}
