package parquet

import (
	"fmt"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/samber/lo"
	"golang.org/x/exp/slices"
)

const (
	DROP    = -1
	KEEP    = 0
	EXTRACT = 1

	podNameKey   = "pod"
	namespaceKey = "namespace"
	containerKey = "container"
	nodeKey      = "node"
)

func createDatapointForLabels(labels []prompb.Label) DataPoint {
	dp := DataPoint{}

	remainder := []prompb.Label{}
	for _, l := range labels {
		switch l.Name {
		case model.MetricNameLabel:
			continue
		case podNameKey:
			dp.Pod = lo.ToPtr(l.Value)
		case namespaceKey:
			dp.Namespace = lo.ToPtr(l.Value)
		case containerKey:
			dp.Container = lo.ToPtr(l.Value)
		case nodeKey:
			dp.Container = lo.ToPtr(l.Value)
		default:
			remainder = append(remainder, l)
		}
	}

	slices.SortFunc(remainder, func(l1, l2 prompb.Label) int { return strings.Compare(l1.Name, l2.Name) })
	labelStrings := lo.Map(remainder, func(l prompb.Label, _ int) string { return fmt.Sprintf("%s=%s", l.Name, l.Value) })

	dp.Labels = strings.Join(labelStrings, ",")

	return dp
}
