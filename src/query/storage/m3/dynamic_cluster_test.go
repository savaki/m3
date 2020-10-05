// Copyright (c) 2020  Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package m3

import (
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xwatch "github.com/m3db/m3/src/x/watch"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	defaultTestNs1ID         = ident.StringID("testns1")
	defaultTestNs2ID         = ident.StringID("testns2")
	defaultTestRetentionOpts = retention.NewOptions().
					SetBufferFuture(10 * time.Minute).
					SetBufferPast(10 * time.Minute).
					SetBlockSize(2 * time.Hour).
					SetRetentionPeriod(48 * time.Hour)
	defaultTestNs2RetentionOpts = retention.NewOptions().
					SetBufferFuture(10 * time.Minute).
					SetBufferPast(10 * time.Minute).
					SetBlockSize(4 * time.Hour).
					SetRetentionPeriod(48 * time.Hour)
	defaultTestAggregationOpts = namespace.NewAggregationOptions().
					SetAggregations([]namespace.Aggregation{namespace.NewUnaggregatedAggregation()})
	defaultTestNs2AggregationOpts = namespace.NewAggregationOptions().
					SetAggregations([]namespace.Aggregation{namespace.NewAggregatedAggregation(
			namespace.AggregatedAttributes{
				Resolution:        1 * time.Minute,
				DownsampleOptions: namespace.NewDownsampleOptions(true),
			}),
		})
	defaultTestNs1Opts = namespace.NewOptions().SetRetentionOptions(defaultTestRetentionOpts).
				SetAggregationOptions(defaultTestAggregationOpts)
	defaultTestNs2Opts = namespace.NewOptions().SetRetentionOptions(defaultTestNs2RetentionOpts).
				SetAggregationOptions(defaultTestNs2AggregationOpts)
)

func TestDynamicClustersInitialization(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)

	mapCh := make(nsMapCh, 10)
	mapCh <- testNamespaceMap(t)
	mockInitializer := newMockNsInitializer(t, ctrl, mapCh)

	cfg := ClusterDynamicConfiguration{
		session:       mockSession,
		nsInitializer: mockInitializer,
	}

	opts := NewDynamicClusterOptions().
		SetClusterDynamicConfiguration([]ClusterDynamicConfiguration{cfg}).
		SetInstrumentOptions(instrument.NewOptions())

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	aggNs, _ := clusters.AggregatedClusterNamespace(RetentionResolution{
		Retention:  48 * time.Hour,
		Resolution: 1 * time.Minute,
	})
	require.Equal(t, defaultTestNs2ID.String(), aggNs.NamespaceID().String())
	require.Equal(t, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Retention:   48 * time.Hour,
			Resolution:  1 * time.Minute,
		},
		downsample: &ClusterNamespaceDownsampleOptions{All: true},
	}, aggNs.Options())

	unaggNs := clusters.UnaggregatedClusterNamespace()
	require.Equal(t, defaultTestNs1ID.String(), unaggNs.NamespaceID().String())
	require.Equal(t, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			Retention:   48 * time.Hour,
		}}, unaggNs.Options())

	nsIds := make([]string, 0, 2)
	for _, ns := range clusters.ClusterNamespaces() {
		nsIds = append(nsIds, ns.NamespaceID().String())
	}
	require.Equal(t, nsIds, []string{defaultTestNs1ID.String(), defaultTestNs2ID.String()})
}

func TestDynamicClustersWithUpdates(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)

	mapCh := make(nsMapCh, 10)
	nsMap := testNamespaceMap(t)
	mapCh <- nsMap
	mockInitializer := newMockNsInitializer(t, ctrl, mapCh)

	cfg := ClusterDynamicConfiguration{
		session:       mockSession,
		nsInitializer: mockInitializer,
	}

	opts := NewDynamicClusterOptions().
		SetClusterDynamicConfiguration([]ClusterDynamicConfiguration{cfg}).
		SetInstrumentOptions(instrument.NewOptions())

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	// Update resolution of aggregated namespace
	md1, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	newOpts := defaultTestNs2Opts.
		SetAggregationOptions(defaultTestNs2AggregationOpts.
			SetAggregations([]namespace.Aggregation{namespace.NewAggregatedAggregation(
				namespace.AggregatedAttributes{
					Resolution:        2 * time.Minute,
					DownsampleOptions: namespace.NewDownsampleOptions(true),
				}),
			}))
	md2, err := namespace.NewMetadata(defaultTestNs2ID, newOpts)
	require.NoError(t, err)
	nsMap, err = namespace.NewMap([]namespace.Metadata{md1, md2})
	require.NoError(t, err)

	// Send update to trigger watch
	mapCh <- nsMap

	select {
	case <-time.After(1 * time.Second):
		require.Fail(t, "failed to receive namespace watch update")
	default:
		if aggNs, ok := clusters.AggregatedClusterNamespace(RetentionResolution{
			Retention:  48 * time.Hour,
			Resolution: 2 * time.Minute,
		}); ok {
			require.Equal(t, defaultTestNs2ID.String(), aggNs.NamespaceID().String())
			require.Equal(t, ClusterNamespaceOptions{
				attributes: storagemetadata.Attributes{
					MetricsType: storagemetadata.AggregatedMetricsType,
					Retention:   48 * time.Hour,
					Resolution:  2 * time.Minute,
				},
				downsample: &ClusterNamespaceDownsampleOptions{All: true},
			}, aggNs.Options())

			nsIds := make([]string, 0, 2)
			for _, ns := range clusters.ClusterNamespaces() {
				nsIds = append(nsIds, ns.NamespaceID().String())
			}
			require.Equal(t, nsIds, []string{defaultTestNs1ID.String(), defaultTestNs2ID.String()})
		}
	}
}
func TestDynamicClustersWithMultipleInitializers(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockSession := client.NewMockSession(ctrl)
	mockSession2 := client.NewMockSession(ctrl)

	mapCh := make(nsMapCh, 10)
	mapCh <- testNamespaceMap(t)
	mockInitializer := newMockNsInitializer(t, ctrl, mapCh)

	md1, err := namespace.NewMetadata(ident.StringID("foo"), defaultTestNs1Opts.
		SetAggregationOptions(namespace.NewAggregationOptions().
			SetAggregations([]namespace.Aggregation{namespace.NewAggregatedAggregation(
				namespace.AggregatedAttributes{
					Resolution:        2 * time.Minute,
					DownsampleOptions: namespace.NewDownsampleOptions(true),
				}),
			})))
	require.NoError(t, err)
	md2, err := namespace.NewMetadata(ident.StringID("bar"), defaultTestNs1Opts.
		SetAggregationOptions(namespace.NewAggregationOptions().
			SetAggregations([]namespace.Aggregation{namespace.NewAggregatedAggregation(
				namespace.AggregatedAttributes{
					Resolution:        5 * time.Minute,
					DownsampleOptions: namespace.NewDownsampleOptions(false),
				}),
			})))
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1, md2})
	require.NoError(t, err)

	mapCh2 := make(nsMapCh, 10)
	mapCh2 <- nsMap
	mockInitializer2 := newMockNsInitializer(t, ctrl, mapCh2)

	cfg := ClusterDynamicConfiguration{
		session:       mockSession,
		nsInitializer: mockInitializer,
	}
	cfg2 := ClusterDynamicConfiguration{
		session:       mockSession2,
		nsInitializer: mockInitializer2,
	}

	opts := NewDynamicClusterOptions().
		SetClusterDynamicConfiguration([]ClusterDynamicConfiguration{cfg, cfg2}).
		SetInstrumentOptions(instrument.NewOptions())

	clusters, err := NewDynamicClusters(opts)
	require.NoError(t, err)

	aggNs1, _ := clusters.AggregatedClusterNamespace(RetentionResolution{
		Retention:  48 * time.Hour,
		Resolution: 1 * time.Minute,
	})
	require.Equal(t, defaultTestNs2ID.String(), aggNs1.NamespaceID().String())
	require.Equal(t, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Retention:   48 * time.Hour,
			Resolution:  1 * time.Minute,
		},
		downsample: &ClusterNamespaceDownsampleOptions{All: true},
	}, aggNs1.Options())

	aggNs2, _ := clusters.AggregatedClusterNamespace(RetentionResolution{
		Retention:  48 * time.Hour,
		Resolution: 2 * time.Minute,
	})
	require.Equal(t, ident.StringID("foo").String(), aggNs2.NamespaceID().String())
	require.Equal(t, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Retention:   48 * time.Hour,
			Resolution:  2 * time.Minute,
		},
		downsample: &ClusterNamespaceDownsampleOptions{All: true},
	}, aggNs2.Options())

	aggNs3, _ := clusters.AggregatedClusterNamespace(RetentionResolution{
		Retention:  48 * time.Hour,
		Resolution: 5 * time.Minute,
	})
	require.Equal(t, ident.StringID("bar").String(), aggNs3.NamespaceID().String())
	require.Equal(t, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			Retention:   48 * time.Hour,
			Resolution:  5 * time.Minute,
		},
		downsample: &ClusterNamespaceDownsampleOptions{All: false},
	}, aggNs3.Options())

	unaggNs := clusters.UnaggregatedClusterNamespace()
	require.Equal(t, defaultTestNs1ID.String(), unaggNs.NamespaceID().String())
	require.Equal(t, ClusterNamespaceOptions{
		attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			Retention:   48 * time.Hour,
		}}, unaggNs.Options())

	nsIds := make([]string, 0, 4)
	for _, ns := range clusters.ClusterNamespaces() {
		nsIds = append(nsIds, ns.NamespaceID().String())
	}
	expected := []string{defaultTestNs1ID.String(), defaultTestNs2ID.String(),
		ident.StringID("foo").String(), ident.StringID("bar").String()}
	sort.Strings(expected)
	sort.Strings(nsIds)
	require.Equal(t, expected, nsIds)
}

// TODO(nate)
// - test error cases
// - test clobbering of unagg or existing agg

func testNamespaceMap(t *testing.T) namespace.Map {
	md1, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	md2, err := namespace.NewMetadata(defaultTestNs2ID, defaultTestNs2Opts)
	require.NoError(t, err)
	nsMap, err := namespace.NewMap([]namespace.Metadata{md1, md2})
	require.NoError(t, err)
	return nsMap
}

type nsMapCh chan namespace.Map

type mockNsInitializer struct {
	registry *namespace.MockRegistry
	updateCh chan struct{}
}

func (m *mockNsInitializer) Init() (namespace.Registry, error) {
	return m.registry, nil
}

func newMockNsInitializer(
	t *testing.T,
	ctrl *gomock.Controller,
	nsMapCh nsMapCh,
) namespace.Initializer {
	updateCh := make(chan struct{}, 10)
	watch := xwatch.NewWatchable()
	go func() {
		for {
			v, ok := <-nsMapCh
			if !ok { // closed channel
				return
			}

			watch.Update(v)
			updateCh <- struct{}{}
		}
	}()

	_, w, err := watch.Watch()
	require.NoError(t, err)

	nsWatch := namespace.NewWatch(w)
	reg := namespace.NewMockRegistry(ctrl)
	reg.EXPECT().Watch().Return(nsWatch, nil).AnyTimes()

	return &mockNsInitializer{
		registry: reg,
		updateCh: updateCh,
	}
}
