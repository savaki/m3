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
	"errors"
	"fmt"
	"sync"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

var (
	errClusterDynamicConfigurationNotSet = errors.New("clusterDynamicConfiguration not set")
	errInstrumentOptionsNotSet           = errors.New("instrumentOptions not set")
	errNsWatchAlreadyClosed              = errors.New("namespace watch already closed")
)

// TODO(nate): rename?
// ClusterDynamicConfiguration is the configuration for
// dynamically connecting to a cluster.
type ClusterDynamicConfiguration struct {
	// session is an active session connected to an M3DB cluster.
	session client.Session

	// nsInitializer is the initializer used to watch for namespace changes.
	nsInitializer namespace.Initializer
}

type dynamicClusterOptions struct {
	config []ClusterDynamicConfiguration
	iOpts  instrument.Options
}

func (d *dynamicClusterOptions) Validate() error {
	if d.config == nil {
		return errClusterDynamicConfigurationNotSet
	}
	if d.iOpts == nil {
		return errInstrumentOptionsNotSet
	}

	return nil
}

func (d *dynamicClusterOptions) SetClusterDynamicConfiguration(value []ClusterDynamicConfiguration) DynamicClusterOptions {
	opts := *d
	opts.config = value
	return &opts
}

func (d *dynamicClusterOptions) ClusterDynamicConfiguration() []ClusterDynamicConfiguration {
	return d.config
}

func (d *dynamicClusterOptions) SetInstrumentOptions(value instrument.Options) DynamicClusterOptions {
	opts := *d
	opts.iOpts = value
	return &opts
}

func (d *dynamicClusterOptions) InstrumentOptions() instrument.Options {
	return d.iOpts
}

// NewDynamicClusterOptions returns new DynamicClusterOptions.
func NewDynamicClusterOptions() DynamicClusterOptions {
	return &dynamicClusterOptions{
		iOpts: instrument.NewOptions(),
	}
}

type clusterNamespacesMap map[namespace.Metadata]ClusterNamespaces

type dynamicCluster struct {
	opts DynamicClusterOptions

	sync.RWMutex

	namespaces              ClusterNamespaces
	unaggregatedNamespace   ClusterNamespace
	aggregatedNamespaces    map[RetentionResolution]ClusterNamespace
	namespacesByEtcdCluster map[int]clusterNamespacesMap

	nsWatches []namespace.NamespaceWatch
	closed    bool
}

// NewDynamicClusters creates an implementation of the Clusters interface
// supports dynamic updating of cluster namespaces.
func NewDynamicClusters(opts DynamicClusterOptions) (Clusters, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	cluster := dynamicCluster{
		opts:                    opts,
		namespacesByEtcdCluster: make(map[int]clusterNamespacesMap),
	}

	if err := cluster.init(); err != nil {
		if err = cluster.Close(); err != nil {
			return nil, err
		}

		return nil, err
	}

	return &cluster, nil
}

func (d *dynamicCluster) init() error {
	logger := d.opts.InstrumentOptions().Logger()

	logger.Info("creating namespaces watches", zap.Int("clusters", len(d.opts.ClusterDynamicConfiguration())))

	var (
		wg       sync.WaitGroup
		multiErr xerrors.MultiError
		errLock  sync.Mutex
	)
	for i, cfg := range d.opts.ClusterDynamicConfiguration() {
		i := i
		cfg := cfg

		wg.Add(1)
		go func() {
			if err := d.initNamespace(i, cfg); err != nil {
				errLock.Lock()
				multiErr = multiErr.Add(err)
				errLock.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()
	if !multiErr.Empty() {
		return multiErr.FinalError()
	}

	return nil
}

func (d *dynamicCluster) initNamespace(etcdClusterId int, cfg ClusterDynamicConfiguration) error {
	// TODO(nate): error on init, log errors on updates
	reg, err := cfg.nsInitializer.Init()
	if err != nil {
		return err
	}

	// get a namespace watch
	watch, err := reg.Watch()
	if err != nil {
		return err
	}

	// Wait till first namespaces value is received and set the value.
	logger := d.opts.InstrumentOptions().Logger()
	logger.Info("resolving namespaces with namespace watch", zap.Int("cluster", etcdClusterId))
	<-watch.C()

	updater := func(namespaces namespace.Map) error {
		return d.updateNamespaces(etcdClusterId, cfg, namespaces)
	}
	nsWatch := namespace.NewNamespaceWatch(updater, watch, d.opts.InstrumentOptions())
	nsMap := watch.Get()
	if err := d.updateNamespaces(etcdClusterId, cfg, nsMap); err != nil {
		// TODO(nate): is this still possible?
		// Log the error and proceed in case some namespace is misconfigured.
		// Misconfigured namespace won't be initialized but should not prevent
		// other namespaces from getting initialized.
		logger.Error("failed to update namespaces", zap.Error(err))
	}

	if err = nsWatch.Start(); err != nil {
		return err
	}

	d.Lock()
	d.nsWatches = append(d.nsWatches, nsWatch)
	d.Unlock()

	return nil
}

func (d *dynamicCluster) updateNamespaces(
	etcdClusterId int,
	clusterCfg ClusterDynamicConfiguration,
	newNamespaces namespace.Map,
) error {
	if newNamespaces == nil {
		return nil
	}

	d.Lock()
	defer d.Unlock()

	d.updateNamespacesByEtcdClusterWithLock(etcdClusterId, clusterCfg, newNamespaces)
	d.updateClusterNamespacesWithLock()

	return nil
}

func (d *dynamicCluster) updateNamespacesByEtcdClusterWithLock(
	etcdClusterId int,
	clusterCfg ClusterDynamicConfiguration,
	newNamespaces namespace.Map,
) {
	// Check if existing namespaces still exist or need to be updated.
	// TODO(nate): info logging on updated namespaces?
	if _, ok := d.namespacesByEtcdCluster[etcdClusterId]; !ok {
		d.namespacesByEtcdCluster[etcdClusterId] = make(clusterNamespacesMap, len(newNamespaces.IDs()))
	}
	existing := d.namespacesByEtcdCluster[etcdClusterId]
	logger := d.opts.InstrumentOptions().Logger()
	for nsMd, _ := range existing {
		nsId := nsMd.ID()
		newNsMd, err := newNamespaces.Get(nsId)

		// TODO(nate): check if new namespace is ready once staging options has landed.
		// Namespace has been removed.
		if err != nil {
			delete(existing, nsMd)
		}

		// Namespace options have been updated; regenerate cluster namespaces.
		if !nsMd.Equal(newNsMd) {
			// Replace with new metadata and cluster namespaces.
			newClusterNamespaces, err := toClusterNamespaces(clusterCfg, newNsMd)
			if err != nil {
				// Log error, but don't allow singular failed namespace update to fail all namespace updates.
				logger.Error("failed to update namespace", zap.String("namespace", nsId.String()),
					zap.Error(err))
				continue
			}
			delete(existing, nsMd)
			existing[newNsMd] = newClusterNamespaces
		}
	}

	// Check for new namespaces to add.
	for _, newNsMd := range newNamespaces.Metadatas() {
		_, ok := existing[newNsMd]
		// Namespace has been added.
		if !ok {
			newClusterNamespaces, err := toClusterNamespaces(clusterCfg, newNsMd)
			if err != nil {
				// Log error, but don't allow singular failed namespace update to fail all namespace updates.
				logger.Error("failed to update namespace", zap.String("namespace", newNsMd.ID().String()),
					zap.Error(err))
				continue
			}
			existing[newNsMd] = newClusterNamespaces
		}
	}
}

func toClusterNamespaces(clusterCfg ClusterDynamicConfiguration, md namespace.Metadata) (ClusterNamespaces, error) {
	aggOpts := md.Options().AggregationOptions()
	if aggOpts == nil {
		return nil, fmt.Errorf("no aggregationOptions present for namespace %v", md.ID().String())
	}

	if len(aggOpts.Aggregations()) == 0 {
		return nil, fmt.Errorf("no aggregations present for namespace %v", md.ID().String())
	}

	retOpts := md.Options().RetentionOptions()
	if retOpts == nil {
		return nil, fmt.Errorf("no retentionOptions present for namespace %v", md.ID().String())
	}

	clusterNamespaces := make(ClusterNamespaces, 0, len(aggOpts.Aggregations()))
	for _, agg := range aggOpts.Aggregations() {
		var (
			clusterNamespace ClusterNamespace
			err              error
		)
		if agg.Aggregated {
			clusterNamespace, err = newAggregatedClusterNamespace(AggregatedClusterNamespaceDefinition{
				NamespaceID: md.ID(),
				Session:     clusterCfg.session,
				Retention:   retOpts.RetentionPeriod(),
				Resolution:  agg.Attributes.Resolution,
				Downsample: &ClusterNamespaceDownsampleOptions{
					All: agg.Attributes.DownsampleOptions.All,
				},
			})
			if err != nil {
				return nil, err
			}
		} else {
			clusterNamespace, err = newUnaggregatedClusterNamespace(UnaggregatedClusterNamespaceDefinition{
				NamespaceID: md.ID(),
				Session:     clusterCfg.session,
				Retention:   retOpts.RetentionPeriod(),
			})
			if err != nil {
				return nil, err
			}
		}
		clusterNamespaces = append(clusterNamespaces, clusterNamespace)
	}

	return clusterNamespaces, nil
}

func (d *dynamicCluster) updateClusterNamespacesWithLock() {
	nsCount := 0
	for _, nsMap := range d.namespacesByEtcdCluster {
		for _, clusterNamespaces := range nsMap {
			nsCount += len(clusterNamespaces)
		}
	}

	var (
		newNamespaces            = make(ClusterNamespaces, 0, nsCount)
		newAggregatedNamespaces  = make(map[RetentionResolution]ClusterNamespace)
		newUnaggregatedNamespace ClusterNamespace
		logger                   = d.opts.InstrumentOptions().Logger()
	)

	for _, nsMap := range d.namespacesByEtcdCluster {
		for _, clusterNamespaces := range nsMap {
			for _, clusterNamespace := range clusterNamespaces {
				attrs := clusterNamespace.Options().Attributes()
				if attrs.MetricsType == storagemetadata.UnaggregatedMetricsType {
					if newUnaggregatedNamespace != nil {
						logger.Warn("more than one unaggregated namespace found. using most recently "+
							"discovered unaggregated namespace",
							zap.String("existing", d.unaggregatedNamespace.NamespaceID().String()),
							zap.String("new", clusterNamespace.NamespaceID().String()))
					}
					newUnaggregatedNamespace = clusterNamespace
				} else {
					retRes := RetentionResolution{
						Retention:  attrs.Retention,
						Resolution: attrs.Resolution,
					}
					existing, ok := newAggregatedNamespaces[retRes]
					if ok {
						logger.Warn("more than one aggregated namespace found for retention and resolution. "+
							"using most recently discovered aggregated namespace",
							zap.String("retention", retRes.Retention.String()),
							zap.String("resolution", retRes.Retention.String()),
							zap.String("existing", existing.NamespaceID().String()),
							zap.String("new", clusterNamespace.NamespaceID().String()))
					}
					newAggregatedNamespaces[retRes] = clusterNamespace
				}
			}
		}
	}

	newNamespaces = append(newNamespaces, newUnaggregatedNamespace)
	for _, ns := range newAggregatedNamespaces {
		newNamespaces = append(newNamespaces, ns)
	}

	d.unaggregatedNamespace = newUnaggregatedNamespace
	d.aggregatedNamespaces = newAggregatedNamespaces
	d.namespaces = newNamespaces
}

func (d *dynamicCluster) Close() error {
	d.Lock()
	defer d.Unlock()

	if d.closed {
		return errNsWatchAlreadyClosed
	}

	d.closed = true

	var multiErr xerrors.MultiError
	for _, watch := range d.nsWatches {
		if err := watch.Close(); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	if !multiErr.Empty() {
		return multiErr.FinalError()
	}

	return nil
}

func (d *dynamicCluster) ClusterNamespaces() ClusterNamespaces {
	d.RLock()
	defer d.RUnlock()

	return d.namespaces
}

func (d *dynamicCluster) UnaggregatedClusterNamespace() ClusterNamespace {
	d.RLock()
	defer d.RUnlock()

	return d.unaggregatedNamespace
}

func (d *dynamicCluster) AggregatedClusterNamespace(attrs RetentionResolution) (ClusterNamespace, bool) {
	d.RLock()
	defer d.RUnlock()

	namespace, ok := d.aggregatedNamespaces[attrs]
	return namespace, ok
}
