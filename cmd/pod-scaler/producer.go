package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/openhistogram/circonusllhist"
	prometheusapi "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"sigs.k8s.io/prow/pkg/interrupts"

	podscaler "github.com/openshift/ci-tools/pkg/pod-scaler"
)

const (
	MetricNameCPUUsage         = `container_cpu_usage_seconds_total`
	MetricNameMemoryWorkingSet = `container_memory_working_set_bytes`

	containerFilter = `{container!="POD",container!=""}`

	// MaxSamplesPerRequest is the maximum number of samples that Prometheus will allow a client to ask for in
	// one request. We also use this to approximate the maximum number of samples we should be asking any one
	// Prometheus server for at once from many requests.
	MaxSamplesPerRequest = 11000

	ProwjobsCachePrefix = "prowjobs"
	PodsCachePrefix     = "pods"
	StepsCachePrefix    = "steps"
)

// queriesByMetric returns a mapping of Prometheus query by metric name for all queries we want to execute
func queriesByMetric() map[string]string {
	queries := map[string]string{}
	for _, info := range []struct {
		prefix   string
		selector string
		labels   []string
	}{
		{
			prefix:   ProwjobsCachePrefix,
			selector: `{` + string(podscaler.ProwLabelNameCreated) + `="true",` + string(podscaler.ProwLabelNameJob) + `!="",` + string(podscaler.LabelNameRehearsal) + `=""}`,
			labels:   []string{string(podscaler.ProwLabelNameCreated), string(podscaler.ProwLabelNameContext), string(podscaler.ProwLabelNameOrg), string(podscaler.ProwLabelNameRepo), string(podscaler.ProwLabelNameBranch), string(podscaler.ProwLabelNameJob), string(podscaler.ProwLabelNameType)},
		},
		{
			prefix:   PodsCachePrefix,
			selector: `{` + string(podscaler.LabelNameCreated) + `="true",` + string(podscaler.LabelNameStep) + `=""}`,
			labels:   []string{string(podscaler.LabelNameOrg), string(podscaler.LabelNameRepo), string(podscaler.LabelNameBranch), string(podscaler.LabelNameVariant), string(podscaler.LabelNameTarget), string(podscaler.LabelNameBuild), string(podscaler.LabelNameRelease), string(podscaler.LabelNameApp)},
		},
		{
			prefix:   StepsCachePrefix,
			selector: `{` + string(podscaler.LabelNameCreated) + `="true",` + string(podscaler.LabelNameStep) + `!=""}`,
			labels:   []string{string(podscaler.LabelNameOrg), string(podscaler.LabelNameRepo), string(podscaler.LabelNameBranch), string(podscaler.LabelNameVariant), string(podscaler.LabelNameTarget), string(podscaler.LabelNameStep)},
		},
	} {
		for name, metric := range map[string]string{
			MetricNameCPUUsage:         `rate(` + MetricNameCPUUsage + containerFilter + `[3m])`,
			MetricNameMemoryWorkingSet: MetricNameMemoryWorkingSet + containerFilter,
		} {
			queries[fmt.Sprintf("%s/%s", info.prefix, name)] = queryFor(metric, info.selector, info.labels)
		}
	}
	return queries
}

func produce(clients map[string]prometheusapi.API, dataCache Cache, ignoreLatest time.Duration, once bool) {
	var execute func(func())
	if once {
		execute = func(f func()) {
			f()
		}
	} else {
		execute = func(f func()) {
			interrupts.TickLiteral(f, 2*time.Hour)
		}
	}
	execute(func() {
		for name, query := range queriesByMetric() {
			name := name
			query := query
			logger := logrus.WithFields(logrus.Fields{
				"version": "v2",
				"metric":  name,
			})
			cache, err := LoadCache(dataCache, name, logger)
			if errors.Is(err, notExist{}) {
				ranges := map[string][]podscaler.TimeRange{}
				for cluster := range clients {
					ranges[cluster] = []podscaler.TimeRange{}
				}
				cache = &podscaler.CachedQuery{
					Query:                  query,
					RangesByCluster:        ranges,
					Data:                   map[model.Fingerprint]*circonusllhist.HistogramWithoutLookups{},
					DataByMetaData:         map[podscaler.FullMetadata][]podscaler.FingerprintTime{},
					NodeSaturationData:     map[model.Fingerprint]podscaler.NodeSaturationInfo{},
					FingerprintToNamespace: map[model.Fingerprint]podscaler.PodIdentifier{},
				}
			} else if err != nil {
				logrus.WithError(err).Error("Failed to load data from storage.")
				continue
			} else {
				// Initialize FingerprintToNamespace map (not persisted, so rebuild on load)
				if cache.FingerprintToNamespace == nil {
					cache.FingerprintToNamespace = make(map[model.Fingerprint]podscaler.PodIdentifier)
				}
			}
			until := time.Now().Add(-ignoreLatest)
			q := querier{
				lock: &sync.RWMutex{},
				data: cache,
			}
			wg := &sync.WaitGroup{}
			for clusterName, client := range clients {
				metadata := &clusterMetadata{
					logger: logger.WithField("cluster", clusterName),
					name:   clusterName,
					client: client,
					lock:   &sync.RWMutex{},
					// there's absolutely no chance Prometheus at the current scaling will ever be able
					// to respond to large requests it's completely capable of creating, so don't even
					// bother asking for anything larger than 1/20th of the largest request we can get
					// responses within the default client connection timeout.
					maxSize: MaxSamplesPerRequest / 20,
					errors:  make(chan error),
					// there's also no chance that Prometheus will be able to handle any real concurrent
					// request volume, so don't even bother trying to request more samples at once than
					// a fifth of the maximum samples it can technically provide in one request
					sync: semaphore.NewWeighted(MaxSamplesPerRequest / 15),
					wg:   &sync.WaitGroup{},
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := q.execute(interrupts.Context(), metadata, until); err != nil {
						metadata.logger.WithError(err).Error("Failed to query Prometheus.")
					}
				}()
			}
			wg.Wait()

			// After collecting container metrics, detect node saturation
			if strings.Contains(name, MetricNameCPUUsage) {
				detectNodeSaturation(clients, cache, until, logger)
			}

			if err := storeCache(dataCache, name, cache, logger); err != nil {
				logger.WithError(err).Error("Failed to write cached data.")
			}
		}
	})
}

// queryFor applies our filtering and left joins to a metric to get data we can use
func queryFor(metric, selector string, labels []string) string {
	return `sum by (
    namespace,
    pod,
    container
  ) (` + metric + `)
  * on(namespace,pod) 
  group_left(
    ` + strings.Join(labels, ",\n    ") + `
  ) max by (
    namespace,
    pod,
    ` + strings.Join(labels, ",\n    ") + `
  ) (kube_pod_labels` + selector + `)`
}

func rangeFrom(r prometheusapi.Range) podscaler.TimeRange {
	return podscaler.TimeRange{
		Start: r.Start,
		End:   r.End,
	}
}

type querier struct {
	lock *sync.RWMutex
	data *podscaler.CachedQuery
}

type clusterMetadata struct {
	logger *logrus.Entry
	name   string
	client prometheusapi.API
	errors chan error

	lock    *sync.RWMutex
	maxSize int64

	// sync guards the number of concurrent samples we can be asking Prometheus for at any one time
	sync *semaphore.Weighted
	wg   *sync.WaitGroup
}

func (q *querier) execute(ctx context.Context, c *clusterMetadata, until time.Time) error {
	runtime, err := c.client.Runtimeinfo(ctx)
	if err != nil {
		return fmt.Errorf("could not query Prometheus runtime info: %w", err)
	}
	storageRetention := runtime.StorageRetention
	// storageRetention may look like "11d or 90Gib" or "30d" depending on the configuration
	parts := strings.Split(storageRetention, " ")
	retention, err := model.ParseDuration(parts[0])
	if err != nil {
		return fmt.Errorf("could not determine Prometheus retention duration: %w", err)
	}
	r := prometheusapi.Range{
		Start: time.Now().Add(-time.Duration(retention)),
		End:   until,
		Step:  1 * time.Minute,
	}

	errLock := &sync.Mutex{}
	var errs []error
	go func() {
		errLock.Lock()
		defer errLock.Unlock()
		for err := range c.errors {
			errs = append(errs, err)
		}
	}()

	q.lock.RLock()
	previousEntries, previousIdentifiers := len(q.data.Data), len(q.data.DataByMetaData)
	q.lock.RUnlock()
	queryStart := time.Now()
	logger := c.logger.WithFields(logrus.Fields{
		"start": r.Start.Format(time.RFC3339),
		"end":   r.End.Format(time.RFC3339),
		"step":  r.Step,
	})
	logger.Info("Initiating queries to Prometheus.")
	uncovered := q.uncoveredRanges(c.name, rangeFrom(r))
	c.lock.RLock()
	numSteps := c.maxSize - 1
	c.lock.RUnlock()
	for _, r := range divideRange(uncovered, r.Step, numSteps) {
		c.wg.Add(1)
		go q.executeOverRange(ctx, c, r)
	}
	c.wg.Wait()
	q.lock.RLock()
	currentEntries, currentIdentifiers := len(q.data.Data), len(q.data.DataByMetaData)
	q.lock.RUnlock()
	logger.Infof("Query completed after %s, yielding %d new identifiers and %d new data series.", time.Since(queryStart).Round(time.Second), currentIdentifiers-previousIdentifiers, currentEntries-previousEntries)
	close(c.errors)
	errLock.Lock()
	return kerrors.NewAggregate(errs)
}

// uncoveredRanges determines the largest subset ranges of r that are not covered by
// existing data in the querier.
func (q *querier) uncoveredRanges(cluster string, r podscaler.TimeRange) []podscaler.TimeRange {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return podscaler.UncoveredRanges(r, q.data.RangesByCluster[cluster])
}

// divideRange divides a range into smaller ranges based on how many samples we think is reasonable
// to ask for from Prometheus in one query
func divideRange(uncovered []podscaler.TimeRange, step time.Duration, numSteps int64) []prometheusapi.Range {
	var divided []prometheusapi.Range
	for _, uncoveredRange := range uncovered {
		// Prometheus has practical limits for how much data we can ask for in any one request,
		// so we take each uncovered range and split it into chunks we can ask for.
		start := uncoveredRange.Start
		stop := uncoveredRange.End
		for {
			if start.After(uncoveredRange.End) {
				break
			}
			steps := int64(stop.Sub(start) / step)
			if steps <= 1 {
				// this range is likely too small to contain novel data and asking for this in a query leads
				// to weird behavior - if we ignore it for now, we won't call it covered and will subsume it
				// into a larger range in the future
				break
			} else if steps > numSteps {
				stop = start.Add(time.Duration(numSteps) * step)
			}
			divided = append(divided, prometheusapi.Range{Start: start, End: stop, Step: step})
			// adding a step to the start will ensure we don't double-count samples on the edge, as ranges are inclusive
			// and the query is evaulated at start, start+step, start+2*step, etc - if we started less than end+step, we
			// would get the same value we got at the end of the previous query
			start = stop.Add(step)
			stop = uncoveredRange.End
		}
	}
	return divided
}

func (q *querier) executeOverRange(ctx context.Context, c *clusterMetadata, r prometheusapi.Range) {
	defer c.wg.Done()
	numSteps := int64(r.End.Sub(r.Start) / r.Step)
	logger := c.logger.WithFields(logrus.Fields{
		"start": r.Start.Format(time.RFC3339),
		"end":   r.End.Format(time.RFC3339),
		"step":  r.Step,
		"steps": numSteps,
	})
	if err := c.sync.Acquire(ctx, numSteps); err != nil {
		c.errors <- err
		return
	}
	defer c.sync.Release(numSteps)
	c.lock.RLock()
	currentMax := c.maxSize
	c.lock.RUnlock()
	subdivide := func() {
		c.wg.Add(2)
		middle := r.Start.Add(time.Duration(numSteps) / 2 * r.Step)
		go q.executeOverRange(ctx, c, prometheusapi.Range{Start: r.Start, End: middle, Step: r.Step})
		go q.executeOverRange(ctx, c, prometheusapi.Range{Start: middle.Add(r.Step), End: r.End, Step: r.Step})
	}
	if numSteps >= currentMax {
		logger.Debugf("Preemptively halving request as prior data shows ours is too large (%d>=%d).", numSteps, currentMax)
		subdivide()
		return
	}

	queryStart := time.Now()
	logger.Debug("Querying Prometheus.")
	q.lock.RLock()
	query := q.data.Query
	q.lock.RUnlock()
	result, warnings, err := c.client.QueryRange(ctx, query, r)
	logger.Debugf("Queried Prometheus API in %s.", time.Since(queryStart).Round(time.Second))
	if err != nil {
		apiError := &prometheusapi.Error{}
		if errors.As(err, &apiError) {
			// Prometheus determined not to expose this programmatically ...
			if strings.HasSuffix(apiError.Msg, "504") {
				var ignoreErrorAndSubdivide bool
				c.lock.Lock()
				if numSteps >= c.maxSize {
					// We hit a timeout asking for a known large value, subdivide our query.
					ignoreErrorAndSubdivide = true
				} else if numSteps > 250 { // implicit: numSteps < c.maxSize
					// We hit a timeout and are still asking for a reasonably "large" amount of
					// data at once, so halve the amount of data we are asking for in order to
					// have a higher chance of getting the data next time. If we're asking for
					// a small amount already it's likely the server is on the verge of falling
					// over, so just error out and try again later.
					logger.Debugf("Received 504 asking for %d samples, halving to %d.", numSteps, numSteps/2)
					c.maxSize = numSteps
					ignoreErrorAndSubdivide = true
				} else {
					logger.Debugf("Received 504 but only asking for %d samples, aborting.", numSteps)
				}
				c.lock.Unlock()
				if ignoreErrorAndSubdivide {
					// the error isn't fatal to the fetch, ignore it and subdivide the query
					subdivide()
					return
				}
			}
		}
		logger.WithError(err).Error("Failed to query Prometheus API.")
		c.errors <- fmt.Errorf("failed to query Prometheus API: %w", err)
		return
	}
	if len(warnings) > 0 {
		logger.WithField("warnings", warnings).Warn("Got warnings from Prometheus.")
	}

	matrix, ok := result.(model.Matrix)
	if !ok {
		c.errors <- fmt.Errorf("returned result of type %T from Prometheus cannot be cast to matrix", result)
		return
	}

	saveStart := time.Now()
	logger.Debug("Saving response from Prometheus data.")
	q.lock.Lock()
	q.data.Record(c.name, rangeFrom(r), matrix, logger)
	q.lock.Unlock()
	logger.Debugf("Saved Prometheus response after %s.", time.Since(saveStart).Round(time.Second))
}

// detectNodeSaturation queries node CPU metrics and marks pods that ran on saturated nodes
func detectNodeSaturation(clients map[string]prometheusapi.API, cache *podscaler.CachedQuery, until time.Time, logger *logrus.Entry) {
	logger.Info("Detecting node saturation for pod executions")

	ctx := interrupts.Context()
	saturationThreshold := 0.8  // 80% CPU utilization
	minSaturationDuration := 60 // seconds

	for clusterName, client := range clients {
		clusterLogger := logger.WithField("cluster", clusterName)

		// Get the time range we're analyzing (same as container metrics)
		runtime, err := client.Runtimeinfo(ctx)
		if err != nil {
			clusterLogger.WithError(err).Warn("Could not query Prometheus runtime info for node saturation detection")
			continue
		}
		storageRetention := runtime.StorageRetention
		parts := strings.Split(storageRetention, " ")
		retention, err := model.ParseDuration(parts[0])
		if err != nil {
			clusterLogger.WithError(err).Warn("Could not determine Prometheus retention duration")
			continue
		}

		start := time.Now().Add(-time.Duration(retention))
		end := until

		// Query 1: Get pod info (namespace, pod, node, start time)
		// kube_pod_info provides the pod-to-node mapping
		podInfoQuery := `kube_pod_info{node!=""}`
		clusterLogger.Debug("Querying kube_pod_info for pod-to-node mapping")

		podInfoResult, warnings, err := client.QueryRange(ctx, podInfoQuery, prometheusapi.Range{
			Start: start,
			End:   end,
			Step:  1 * time.Minute,
		})

		if err != nil {
			clusterLogger.WithError(err).Warn("Failed to query kube_pod_info")
			continue
		}
		if len(warnings) > 0 {
			clusterLogger.WithField("warnings", warnings).Debug("Got warnings from kube_pod_info query")
		}

		podInfoMatrix, ok := podInfoResult.(model.Matrix)
		if !ok {
			clusterLogger.Warn("kube_pod_info result is not a matrix")
			continue
		}

		// Build pod-to-node mapping with timing
		type podInfo struct {
			namespace string
			pod       string
			node      string
			startTime time.Time
			endTime   time.Time
		}

		podToNode := make(map[string]podInfo) // key: "namespace/pod"
		for _, stream := range podInfoMatrix {
			namespace := string(stream.Metric["namespace"])
			pod := string(stream.Metric["pod"])
			node := string(stream.Metric["node"])

			if namespace == "" || pod == "" || node == "" {
				continue
			}

			key := fmt.Sprintf("%s/%s", namespace, pod)

			// Get time range from values
			if len(stream.Values) > 0 {
				startTime := stream.Values[0].Timestamp.Time()
				endTime := stream.Values[len(stream.Values)-1].Timestamp.Time()

				podToNode[key] = podInfo{
					namespace: namespace,
					pod:       pod,
					node:      node,
					startTime: startTime,
					endTime:   endTime,
				}
			}
		}

		clusterLogger.Debugf("Mapped %d pods to nodes", len(podToNode))

		// Query 2: Get node CPU utilization
		// Calculate CPU utilization: 1 - avg(idle_time)
		nodeCPUQuery := `1 - avg by (node) (rate(node_cpu_seconds_total{mode="idle"}[1m]))`
		clusterLogger.Debug("Querying node CPU utilization")

		nodeCPUResult, warnings, err := client.QueryRange(ctx, nodeCPUQuery, prometheusapi.Range{
			Start: start,
			End:   end,
			Step:  1 * time.Minute,
		})

		if err != nil {
			clusterLogger.WithError(err).Warn("Failed to query node CPU metrics")
			continue
		}
		if len(warnings) > 0 {
			clusterLogger.WithField("warnings", warnings).Debug("Got warnings from node CPU query")
		}

		nodeCPUMatrix, ok := nodeCPUResult.(model.Matrix)
		if !ok {
			clusterLogger.Warn("Node CPU result is not a matrix")
			continue
		}

		// Build node CPU timeline: node -> (time -> cpuUtilization)
		nodeCPUTimeline := make(map[string]map[time.Time]float64)
		for _, stream := range nodeCPUMatrix {
			node := string(stream.Metric["node"])
			if node == "" {
				continue
			}

			if nodeCPUTimeline[node] == nil {
				nodeCPUTimeline[node] = make(map[time.Time]float64)
			}

			for _, value := range stream.Values {
				nodeCPUTimeline[node][value.Timestamp.Time()] = float64(value.Value)
			}
		}

		clusterLogger.Debugf("Collected CPU data for %d nodes", len(nodeCPUTimeline))

		// Correlate: For each pod in our cache, check if its node was saturated
		saturatedCount := 0
		checkedCount := 0

		for meta, fingerprintTimes := range cache.DataByMetaData {
			for i, ft := range fingerprintTimes {
				// Get namespace/pod for this fingerprint
				namespace, pod, exists := findPodForFingerprint(cache, ft.Fingerprint)
				if !exists || namespace == "" || pod == "" {
					continue
				}

				podKey := fmt.Sprintf("%s/%s", namespace, pod)
				podInf, hasPodInfo := podToNode[podKey]
				if !hasPodInfo {
					continue
				}

				checkedCount++

				// Check if this pod's node was saturated during pod lifetime
				nodeCPU, hasNodeCPU := nodeCPUTimeline[podInf.node]
				if !hasNodeCPU {
					continue
				}

				// Check for sustained saturation (>80% for >1 minute)
				wasSaturated, maxCPU := checkNodeSaturation(nodeCPU, podInf.startTime, podInf.endTime, saturationThreshold, minSaturationDuration)

				if wasSaturated {
					saturatedCount++

					// Update the fingerprint time
					cache.DataByMetaData[meta][i].NodeSaturated = true

					// Update node saturation data
					if cache.NodeSaturationData == nil {
						cache.NodeSaturationData = make(map[model.Fingerprint]podscaler.NodeSaturationInfo)
					}
					cache.NodeSaturationData[ft.Fingerprint] = podscaler.NodeSaturationInfo{
						WasSaturated: true,
						NodeName:     podInf.node,
						MaxNodeCPU:   maxCPU,
					}
				}
			}
		}

		clusterLogger.WithFields(logrus.Fields{
			"checked":   checkedCount,
			"saturated": saturatedCount,
		}).Info("Completed node saturation detection")
	}
}

// findPodForFingerprint finds the namespace/pod associated with a fingerprint
func findPodForFingerprint(cache *podscaler.CachedQuery, fingerprint model.Fingerprint) (string, string, bool) {
	if cache.FingerprintToNamespace == nil {
		return "", "", false
	}

	podIdent, exists := cache.FingerprintToNamespace[fingerprint]
	if !exists {
		return "", "", false
	}

	return podIdent.Namespace, podIdent.Pod, true
}

// checkNodeSaturation determines if a node was saturated during a time period
// Returns (wasSaturated bool, maxCPU float64)
func checkNodeSaturation(nodeCPU map[time.Time]float64, podStart, podEnd time.Time, threshold float64, minDurationSeconds int) (bool, float64) {
	// Get CPU samples during pod lifetime
	var samplesInRange []struct {
		time time.Time
		cpu  float64
	}

	maxCPU := 0.0
	for t, cpu := range nodeCPU {
		if (t.Equal(podStart) || t.After(podStart)) && (t.Equal(podEnd) || t.Before(podEnd)) {
			samplesInRange = append(samplesInRange, struct {
				time time.Time
				cpu  float64
			}{t, cpu})

			if cpu > maxCPU {
				maxCPU = cpu
			}
		}
	}

	if len(samplesInRange) == 0 {
		return false, 0
	}

	// Sort by time
	sort.Slice(samplesInRange, func(i, j int) bool {
		return samplesInRange[i].time.Before(samplesInRange[j].time)
	})

	// Check for sustained saturation (consecutive samples above threshold)
	consecutiveSaturatedSeconds := 0
	for _, sample := range samplesInRange {
		if sample.cpu >= threshold {
			consecutiveSaturatedSeconds += 60 // Each sample is 1 minute apart
			if consecutiveSaturatedSeconds >= minDurationSeconds {
				return true, maxCPU
			}
		} else {
			consecutiveSaturatedSeconds = 0
		}
	}

	return false, maxCPU
}
