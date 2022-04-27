// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package owner

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner/migration"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/version"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type ownerJobType int

// All OwnerJob types
const (
	ownerJobTypeRebalance ownerJobType = iota
	ownerJobTypeScheduleTable
	ownerJobTypeAdminJob
	ownerJobTypeDebugInfo
	ownerJobTypeQuery
)

// versionInconsistentLogRate represents the rate of log output when there are
// captures with versions different from that of the owner
const versionInconsistentLogRate = 1

// Export field names for pretty printing.
type ownerJob struct {
	Tp           ownerJobType
	ChangefeedID model.ChangeFeedID

	// for ScheduleTable only
	TargetCaptureID model.CaptureID
	// for ScheduleTable only
	TableID model.TableID

	// for Admin Job only
	AdminJob *model.AdminJob

	// for debug info only
	debugInfoWriter io.Writer

	// for status provider
	query *Query

	done chan<- error
}

// Owner managers TiCDC cluster.
//
// The interface is thread-safe, except for Tick, it's only used by etcd worker.
type Owner interface {
	orchestrator.Reactor
	EnqueueJob(adminJob model.AdminJob, done chan<- error)
	RebalanceTables(cfID model.ChangeFeedID, done chan<- error)
	ScheduleTable(
		cfID model.ChangeFeedID, toCapture model.CaptureID,
		tableID model.TableID, done chan<- error,
	)
	WriteDebugInfo(w io.Writer, done chan<- error)
	Query(query *Query, done chan<- error)
	AsyncStop()
}

type ownerImpl struct {
	// 从 clusterID 映射到 changefeeds map
	// changefeeds map[uint64]map[model.ChangeFeedID]*changefeed
	changefeeds map[model.ChangeFeedID]*changefeed
	captures    map[model.CaptureID]*model.CaptureInfo

	ownerJobQueue struct {
		sync.Mutex
		queue []*ownerJob
	}
	// logLimiter controls cluster version check log output rate
	logLimiter   *rate.Limiter
	lastTickTime time.Time
	closed       int32
	// bootstrapped specifies whether the owner has been initialized.
	// This will only be done when the owner starts the first Tick.
	// NOTICE: Do not use it in a method other than tick unexpectedly,
	//         as it is not a thread-safe value.
	bootstrapped bool

	cli *etcd.CDCEtcdClient

	newChangefeed func(id model.ChangeFeedID, upStream *upstream.UpStream) *changefeed
}

// NewOwner creates a new Owner
func NewOwner(cli *etcd.CDCEtcdClient) Owner {
	return &ownerImpl{
		changefeeds: make(map[model.ChangeFeedID]*changefeed),
		// gcManager:     gc.NewManager(pdClient),
		lastTickTime:  time.Now(),
		newChangefeed: newChangefeed,
		logLimiter:    rate.NewLimiter(versionInconsistentLogRate, versionInconsistentLogRate),
		cli:           cli,
	}
}

// NewOwner4Test creates a new Owner for test
func NewOwner4Test(
	newDDLPuller func(ctx cdcContext.Context, upStream *upstream.UpStream, startTs uint64) (DDLPuller, error),
	newSink func() DDLSink,
	pdClient pd.Client,
) Owner {
	o := NewOwner(nil).(*ownerImpl)
	// Most tests do not need to test bootstrap.
	o.bootstrapped = true
	o.newChangefeed = func(id model.ChangeFeedID, upStream *upstream.UpStream) *changefeed {
		return newChangefeed4Test(id, upStream, newDDLPuller, newSink)
	}
	return o
}

// Tick implements the Reactor interface
func (o *ownerImpl) Tick(stdCtx context.Context, rawState orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	failpoint.Inject("owner-run-with-error", func() {
		failpoint.Return(nil, errors.New("owner run with injected error"))
	})
	failpoint.Inject("sleep-in-owner-tick", nil)
	state := rawState.(*orchestrator.GlobalReactorState)
	// At the first Tick, we need to do a bootstrap operation.
	// Fix incompatible or incorrect meta information.
	if !o.bootstrapped {
		done, err := migration.MigrateData(stdCtx, o.cli)
		if !done {
			return state, errors.Trace(err)
		}
		o.Bootstrap(state)
		o.bootstrapped = true
		return state, nil
	}

	o.captures = state.Captures
	o.updateMetrics(state)

	// handleJobs() should be called before clusterVersionConsistent(), because
	// when there are different versions of cdc nodes in the cluster,
	// the admin job may not be processed all the time. And http api relies on
	// admin job, which will cause all http api unavailable.
	o.handleJobs()

	if !o.clusterVersionConsistent(state.Captures) {
		return state, nil
	}

	for key, info := range state.Upstreams {
		if err := upstream.UpStreamManager.TryInit(key, info); err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Owner should update GC safepoint before initializing changefeed, so
	// changefeed can remove its "ticdc-creating" service GC safepoint during
	// initializing.
	//
	// See more gc doc.
	if err = o.updateGCSafepoint(stdCtx, state); err != nil {
		return nil, errors.Trace(err)
	}

	// Tick all changefeeds.
	ctx := stdCtx.(cdcContext.Context)
	for changefeedID, changefeedState := range state.Changefeeds {
		if changefeedState.Info == nil {
			o.cleanUpChangefeed(changefeedState)
			if cfReactor, ok := o.changefeeds[changefeedID]; ok {
				cfReactor.isRemoved = true
			}
			continue
		}
		ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
			ID:   changefeedID,
			Info: changefeedState.Info,
		})
		cfReactor, exist := o.changefeeds[changefeedID]
		if !exist {
			// 需要获取 clusterID 来划分 changefeed 所属集群
			upStream, err := upstream.UpStreamManager.Get(changefeedState.Info.UpstreamID)
			if err != nil {
				return state, errors.Trace(err)
			}
			cfReactor = o.newChangefeed(changefeedID, upStream)
			o.changefeeds[changefeedID] = cfReactor
		}
		cfReactor.Tick(ctx, changefeedState, state.Captures)
	}

	// Cleanup changefeeds that are not in the state.
	if len(o.changefeeds) != len(state.Changefeeds) {
		for changefeedID, cfReactor := range o.changefeeds {
			if _, exist := state.Changefeeds[changefeedID]; exist {
				continue
			}
			ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
				ID: changefeedID,
			})
			cfReactor.Close(ctx)
			delete(o.changefeeds, changefeedID)
		}
	}

	// Close and cleanup all changefeeds.
	if atomic.LoadInt32(&o.closed) != 0 {
		for changefeedID, cfReactor := range o.changefeeds {
			ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
				ID: changefeedID,
			})
			cfReactor.Close(ctx)
		}
		return state, cerror.ErrReactorFinished.GenWithStackByArgs()
	}

	return state, nil
}

// EnqueueJob enqueues an admin job into an internal queue,
// and the Owner will handle the job in the next tick
// `done` must be buffered to prevent blocking owner.
func (o *ownerImpl) EnqueueJob(adminJob model.AdminJob, done chan<- error) {
	o.pushOwnerJob(&ownerJob{
		Tp:           ownerJobTypeAdminJob,
		AdminJob:     &adminJob,
		ChangefeedID: adminJob.CfID,
		done:         done,
	})
}

// RebalanceTables triggers a rebalance for the specified changefeed
// `done` must be buffered to prevent blocking owner.
func (o *ownerImpl) RebalanceTables(cfID model.ChangeFeedID, done chan<- error) {
	o.pushOwnerJob(&ownerJob{
		Tp:           ownerJobTypeRebalance,
		ChangefeedID: cfID,
		done:         done,
	})
}

// ScheduleTable moves a table from a capture to another capture
// `done` must be buffered to prevent blocking owner.
func (o *ownerImpl) ScheduleTable(
	cfID model.ChangeFeedID, toCapture model.CaptureID, tableID model.TableID,
	done chan<- error,
) {
	o.pushOwnerJob(&ownerJob{
		Tp:              ownerJobTypeScheduleTable,
		ChangefeedID:    cfID,
		TargetCaptureID: toCapture,
		TableID:         tableID,
		done:            done,
	})
}

// WriteDebugInfo writes debug info into the specified http writer
func (o *ownerImpl) WriteDebugInfo(w io.Writer, done chan<- error) {
	o.pushOwnerJob(&ownerJob{
		Tp:              ownerJobTypeDebugInfo,
		debugInfoWriter: w,
		done:            done,
	})
}

// Query queries owner internal information.
func (o *ownerImpl) Query(query *Query, done chan<- error) {
	o.pushOwnerJob(&ownerJob{
		Tp:    ownerJobTypeQuery,
		query: query,
		done:  done,
	})
}

// AsyncStop stops the owner asynchronously
func (o *ownerImpl) AsyncStop() {
	atomic.StoreInt32(&o.closed, 1)
	o.cleanStaleMetrics()
}

func (o *ownerImpl) cleanUpChangefeed(state *orchestrator.ChangefeedReactorState) {
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return nil, info != nil, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return nil, status != nil, nil
	})
	for captureID := range state.TaskStatuses {
		state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			return nil, status != nil, nil
		})
	}
	for captureID := range state.TaskPositions {
		state.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return nil, position != nil, nil
		})
	}
	for captureID := range state.Workloads {
		state.PatchTaskWorkload(captureID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
			return nil, workload != nil, nil
		})
	}
}

// Bootstrap checks if the state contains incompatible or incorrect information and tries to fix it.
func (o *ownerImpl) Bootstrap(state *orchestrator.GlobalReactorState) {
	log.Info("Start bootstrapping")
	o.cleanStaleMetrics()
	fixChangefeedInfos(state)
}

// fixChangefeedInfos attempts to fix incompatible or incorrect meta information in changefeed state.
func fixChangefeedInfos(state *orchestrator.GlobalReactorState) {
	for _, changefeedState := range state.Changefeeds {
		if changefeedState != nil {
			changefeedState.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
				if info == nil {
					return nil, false, nil
				}
				info.FixIncompatible()
				return info, true, nil
			})
		}
	}
}

func (o *ownerImpl) cleanStaleMetrics() {
	// The gauge metrics of the Owner should be reset
	// each time a new owner is launched, in case the previous owner
	// has crashed and has not cleaned up the stale metrics values.
	changefeedCheckpointTsGauge.Reset()
	changefeedCheckpointTsLagGauge.Reset()
	changefeedResolvedTsGauge.Reset()
	changefeedResolvedTsLagGauge.Reset()
	ownerMaintainTableNumGauge.Reset()
	changefeedStatusGauge.Reset()
}

func (o *ownerImpl) updateMetrics(state *orchestrator.GlobalReactorState) {
	// Keep the value of prometheus expression `rate(counter)` = 1
	// Please also change alert rule in ticdc.rules.yml when change the expression value.
	now := time.Now()
	ownershipCounter.Add(float64(now.Sub(o.lastTickTime)) / float64(time.Second))
	o.lastTickTime = now

	conf := config.GetGlobalServerConfig()

	// TODO refactor this piece of code when the new scheduler is stabilized,
	// and the old scheduler is removed.
	if conf.Debug != nil && conf.Debug.EnableNewScheduler {
		for cfID, cf := range o.changefeeds {
			if cf.state != nil && cf.state.Info != nil {
				changefeedStatusGauge.WithLabelValues(cfID.String()).Set(float64(cf.state.Info.State.ToInt()))
			}

			// The InfoProvider is a proxy object returning information
			// from the scheduler.
			infoProvider := cf.GetInfoProvider()
			if infoProvider == nil {
				// The scheduler has not been initialized yet.
				continue
			}

			totalCounts := infoProvider.GetTotalTableCounts()
			pendingCounts := infoProvider.GetPendingTableCounts()

			for captureID, info := range o.captures {
				ownerMaintainTableNumGauge.
					WithLabelValues(cfID.String(), info.AdvertiseAddr, maintainTableTypeTotal).
					Set(float64(totalCounts[captureID]))
				ownerMaintainTableNumGauge.
					WithLabelValues(cfID.String(), info.AdvertiseAddr, maintainTableTypeWip).
					Set(float64(pendingCounts[captureID]))
			}
		}
		return
	}

	for changefeedID, changefeedState := range state.Changefeeds {
		for captureID, captureInfo := range state.Captures {
			taskStatus, exist := changefeedState.TaskStatuses[captureID]
			if !exist {
				continue
			}
			ownerMaintainTableNumGauge.
				WithLabelValues(changefeedID.String(), captureInfo.AdvertiseAddr, maintainTableTypeTotal).
				Set(float64(len(taskStatus.Tables)))
			ownerMaintainTableNumGauge.
				WithLabelValues(changefeedID.String(), captureInfo.AdvertiseAddr, maintainTableTypeWip).
				Set(float64(len(taskStatus.Operation)))
			if changefeedState.Info != nil {
				changefeedStatusGauge.WithLabelValues(changefeedID.String()).Set(float64(changefeedState.Info.State.ToInt()))
			}
		}
	}
}

func (o *ownerImpl) clusterVersionConsistent(captures map[model.CaptureID]*model.CaptureInfo) bool {
	myVersion := version.ReleaseVersion
	for _, capture := range captures {
		if myVersion != capture.Version {
			if o.logLimiter.Allow() {
				log.Warn("the capture version is different with the owner",
					zap.Reflect("capture", capture), zap.String("ownerVer", myVersion))
			}
			return false
		}
	}
	return true
}

func (o *ownerImpl) handleJobs() {
	jobs := o.takeOwnerJobs()
	for _, job := range jobs {
		changefeedID := job.ChangefeedID
		cfReactor, exist := o.changefeeds[changefeedID]
		if !exist && job.Tp != ownerJobTypeQuery {
			log.Warn("changefeed not found when handle a job", zap.Reflect("job", job))
			job.done <- cerror.ErrChangeFeedNotExists.FastGenByArgs(job.ChangefeedID)
			close(job.done)
			continue
		}
		switch job.Tp {
		case ownerJobTypeAdminJob:
			cfReactor.feedStateManager.PushAdminJob(job.AdminJob)
		case ownerJobTypeScheduleTable:
			cfReactor.scheduler.MoveTable(job.TableID, job.TargetCaptureID)
		case ownerJobTypeRebalance:
			cfReactor.scheduler.Rebalance()
		case ownerJobTypeQuery:
			job.done <- o.handleQueries(job.query)
		case ownerJobTypeDebugInfo:
			// TODO: implement this function
		}
		close(job.done)
	}
}

func (o *ownerImpl) handleQueries(query *Query) error {
	switch query.Tp {
	case QueryAllChangeFeedStatuses:
		ret := map[model.ChangeFeedID]*model.ChangeFeedStatus{}
		for cfID, cfReactor := range o.changefeeds {
			ret[cfID] = &model.ChangeFeedStatus{}
			if cfReactor.state == nil {
				continue
			}
			if cfReactor.state.Status == nil {
				continue
			}
			ret[cfID].ResolvedTs = cfReactor.state.Status.ResolvedTs
			ret[cfID].CheckpointTs = cfReactor.state.Status.CheckpointTs
			ret[cfID].AdminJobType = cfReactor.state.Status.AdminJobType
		}
		query.Data = ret
	case QueryAllChangeFeedInfo:
		ret := map[model.ChangeFeedID]*model.ChangeFeedInfo{}
		for cfID, cfReactor := range o.changefeeds {
			if cfReactor.state == nil {
				continue
			}
			if cfReactor.state.Info == nil {
				ret[cfID] = &model.ChangeFeedInfo{}
				continue
			}
			var err error
			ret[cfID], err = cfReactor.state.Info.Clone()
			if err != nil {
				return errors.Trace(err)
			}
		}
		query.Data = ret
	case QueryAllTaskStatuses:
		cfReactor, ok := o.changefeeds[query.ChangeFeedID]
		if !ok {
			return cerror.ErrChangeFeedNotExists.GenWithStackByArgs(query.ChangeFeedID)
		}
		if cfReactor.state == nil {
			return cerror.ErrChangeFeedNotExists.GenWithStackByArgs(query.ChangeFeedID)
		}

		var ret map[model.CaptureID]*model.TaskStatus
		if provider := cfReactor.GetInfoProvider(); provider != nil {
			// If the new scheduler is enabled, provider should be non-nil.
			var err error
			ret, err = provider.GetTaskStatuses()
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			ret = map[model.CaptureID]*model.TaskStatus{}
			for captureID, taskStatus := range cfReactor.state.TaskStatuses {
				ret[captureID] = taskStatus.Clone()
			}
		}
		query.Data = ret
	case QueryTaskPositions:
		cfReactor, ok := o.changefeeds[query.ChangeFeedID]
		if !ok {
			return cerror.ErrChangeFeedNotExists.GenWithStackByArgs(query.ChangeFeedID)
		}

		var ret map[model.CaptureID]*model.TaskPosition
		if provider := cfReactor.GetInfoProvider(); provider != nil {
			// If the new scheduler is enabled, provider should be non-nil.
			var err error
			ret, err = provider.GetTaskPositions()
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			if cfReactor.state == nil {
				return cerror.ErrChangeFeedNotExists.GenWithStackByArgs(query.ChangeFeedID)
			}
			ret = map[model.CaptureID]*model.TaskPosition{}
			for captureID, taskPosition := range cfReactor.state.TaskPositions {
				ret[captureID] = taskPosition.Clone()
			}
		}
		query.Data = ret
	case QueryProcessors:
		var ret []*model.ProcInfoSnap
		for cfID, cfReactor := range o.changefeeds {
			if cfReactor.state == nil {
				continue
			}
			for captureID := range cfReactor.state.TaskStatuses {
				ret = append(ret, &model.ProcInfoSnap{
					CfID:      cfID,
					CaptureID: captureID,
				})
			}
		}
		query.Data = ret
	case QueryCaptures:
		var ret []*model.CaptureInfo
		for _, captureInfo := range o.captures {
			ret = append(ret, &model.CaptureInfo{
				ID:            captureInfo.ID,
				AdvertiseAddr: captureInfo.AdvertiseAddr,
				Version:       captureInfo.Version,
			})
		}
		query.Data = ret
	}
	return nil
}

func (o *ownerImpl) takeOwnerJobs() []*ownerJob {
	o.ownerJobQueue.Lock()
	defer o.ownerJobQueue.Unlock()

	jobs := o.ownerJobQueue.queue
	o.ownerJobQueue.queue = nil
	return jobs
}

func (o *ownerImpl) pushOwnerJob(job *ownerJob) {
	o.ownerJobQueue.Lock()
	defer o.ownerJobQueue.Unlock()
	o.ownerJobQueue.queue = append(o.ownerJobQueue.queue, job)
}

// 这个函数的逻辑需要修改，owner 应该分别遍历不同上游的 changefeed，然后再计算
func (o *ownerImpl) updateGCSafepoint(
	ctx context.Context, state *orchestrator.GlobalReactorState,
) error {
	minCheckpointTsMap := make(map[string]uint64)
	forceUpdateMap := make(map[string]bool)
	for changefeedID, changefeedState := range state.Changefeeds {
		if changefeedState.Info == nil {
			continue
		}
		switch changefeedState.Info.State {
		case model.StateNormal, model.StateStopped, model.StateError:
		default:
			continue
		}
		checkpointTs := changefeedState.Info.GetCheckpointTs(changefeedState.Status)
		minCheckpointTs, ok := minCheckpointTsMap[changefeedState.Info.UpstreamID]
		if !ok {
			minCheckpointTs = uint64(math.MaxUint64)
		}
		if minCheckpointTs > checkpointTs {
			minCheckpointTs = checkpointTs
		}
		minCheckpointTsMap[changefeedState.Info.UpstreamID] = minCheckpointTs
		// Force update when adding a new changefeed.
		_, exist := o.changefeeds[changefeedID]
		if !exist {
			forceUpdateMap[changefeedState.Info.UpstreamID] = true
		}
	}
	for id, minCheckpointTs := range minCheckpointTsMap {
		// 此处逻辑需要修改, 需要根据上游的不同来更新 safePoint
		upStream, err := upstream.UpStreamManager.Get(id)
		if err != nil {
			return errors.Trace(err)
		}
		if upStream == nil {
			log.Panic("upStream is nil")
		}
		if !upStream.IsInitialized() {
			log.Panic("upStream not initialized")
		}
		if upStream.GCManager == nil {
			log.Panic("gcManager is nil")
		}
		// When the changefeed starts up, CDC will do a snapshot read at
		// (checkpointTs - 1) from TiKV, so (checkpointTs - 1) should be an upper
		// bound for the GC safepoint.
		gcSafepointUpperBound := minCheckpointTs - 1
		err = upStream.GCManager.TryUpdateGCSafePoint(ctx, fmt.Sprintf("%s-%s", config.GetGlobalServerConfig().ClusterID, id), gcSafepointUpperBound, forceUpdateMap[id])
	}
	return nil
}

// StatusProvider returns a StatusProvider
func (o *ownerImpl) StatusProvider() StatusProvider {
	return &ownerStatusProvider{owner: o}
}
