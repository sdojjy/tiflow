// Copyright 2023 PingCAP, Inc.
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

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	ormUtil "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/election"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

// CaptureOb is an implement for metadata.CaptureObservation.
type CaptureOb[T TxnContext] struct {
	// election related fields.
	metadata.Elector
	selfInfo *model.CaptureInfo
	// TODO(CharlesCheung): handle ctx properly.
	egCtx context.Context

	client client[T]

	tasks entity[metadata.ChangefeedUUID, *ScheduleDO]

	// TODO: remove processorChanges.
	ownerChanges     *chann.DrainableChann[metadata.ScheduledChangefeed]
	processorChanges *chann.DrainableChann[metadata.ScheduledChangefeed]
}

// NewCaptureObservation creates a capture observation.
func NewCaptureObservation(
	backendDB *sql.DB, selfInfo *model.CaptureInfo,
) (*CaptureOb[*gorm.DB], error) {
	db, err := ormUtil.NewGormDB(backendDB, "mysql")
	if err != nil {
		return nil, err
	}
	electionStorage, err := election.NewORMStorage(db, "election")
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &CaptureOb[*gorm.DB]{
		selfInfo:         selfInfo,
		client:           NewORMClient(selfInfo.ID, db),
		Elector:          metadata.NewElector(selfInfo, electionStorage),
		ownerChanges:     chann.NewAutoDrainChann[metadata.ScheduledChangefeed](),
		processorChanges: chann.NewAutoDrainChann[metadata.ScheduledChangefeed](),
	}, nil
}

// Run runs the given CaptureOb.
func (c *CaptureOb[T]) Run(
	egCtx context.Context,
	controllerCallback func(context.Context, metadata.ControllerObservation) error,
) (err error) {
	eg, egCtx := errgroup.WithContext(egCtx)
	c.egCtx = egCtx

	eg.Go(func() error {
		return c.Elector.RunElection(egCtx, c.onTakeControl(controllerCallback))
	})

	eg.Go(func() error {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-egCtx.Done():
				err := egCtx.Err()
				log.Info("capture stops handle task changes", zap.String("capture", c.selfInfo.ID), zap.Error(err))
				return err
			case <-ticker.C:
				if err := c.handleTaskChanges(egCtx); err != nil {
					log.Warn("capture handle task changes fail", zap.String("capture", c.selfInfo.ID), zap.Error(err))
					return err
				}
			}
		}
	})

	// TODO: add runWithEg function to reduce the wait goroutine.
	return eg.Wait()
}

func (c *CaptureOb[T]) onTakeControl(
	controllerCallback func(context.Context, metadata.ControllerObservation) error,
) func(context.Context) error {
	return func(ctx context.Context) error {
		checker, ok := c.Elector.(LeaderChecker[T])
		if !ok {
			return errors.New("capture elector is not a leader checker")
		}
		controllerOb := newControllerObservation(checker, c.client, c.selfInfo, c.getAllCaptures)

		eg, egCtx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			return controllerOb.run(egCtx)
		})
		eg.Go(func() error {
			return controllerCallback(egCtx, controllerOb)
		})
		return eg.Wait()
	}
}

func (c *CaptureOb[T]) handleTaskChanges(ctx context.Context) error {
	var err error
	var schedItems []*ScheduleDO

	err = c.client.Txn(ctx, func(tx T) error {
		schedItems, err = c.client.querySchedulesByOwnerIDAndUpdateAt(tx, c.Self().ID, time.Time{})
		return err
	})
	if err != nil {
		return errors.Trace(err)
	}

	if len(schedItems) == 0 {
		// No scheudle information for the capture.
		return nil
	}

	c.tasks.doUpsert(schedItems, func(newV *ScheduleDO) (skip bool) {
		if newV.OwnerState == metadata.SchedRemoved {
			return true
		}
		c.ownerChanges.In() <- newV.ScheduledChangefeed
		c.processorChanges.In() <- newV.ScheduledChangefeed
		return false
	})

	return nil
}

func (c *CaptureOb[T]) Advance(cp metadata.CaptureProgress) error {
	return c.client.Txn(c.egCtx, func(tx T) error {
		return c.client.updateProgress(tx, &ProgressDO{
			CaptureID: c.selfInfo.ID,
			Progress:  &cp,
		})
	})
}

func (c *CaptureOb[T]) OwnerChanges() <-chan metadata.ScheduledChangefeed {
	return c.ownerChanges.Out()
}

func (c *CaptureOb[T]) PostOwnerRemoved(cf metadata.ChangefeedUUID) error {
	sc := c.tasks.get(cf)
	if sc == nil {
		errMsg := fmt.Sprintf("remove owner for a changefeed %d that is not owned by the capture", cf)
		return errors.ErrInconsistentMetaCache.GenWithStackByArgs(errMsg)
	}
	return c.client.TxnWithOwnerLock(c.egCtx, cf, func(tx T) error {
		return c.client.updateScheduleOwnerState(tx, sc)
	})
}

func (c *CaptureOb[T]) ProcessorChanges() <-chan metadata.ScheduledChangefeed {
	return c.processorChanges.Out()
}

func (c *CaptureOb[T]) PostProcessorRemoved(cf metadata.ChangefeedIdent) error {
	c.storage.Lock()
	defer c.storage.Unlock()
	if processors, ok := c.storage.schedule.processors[cf]; ok {
		processors.version += 1
		// processors.v = postProcessorRemoved(processors.v, c.Self().ID)
		c.storage.schedule.processors[cf] = processors
	}
	if processors, ok := c.storage.schedule.processorsByCapture[c.Self().ID]; ok {
		processors.version += 1
		// processors.v = postProcessorRemoved(processors.v, c.Self().ID)
		c.storage.schedule.processorsByCapture[c.Self().ID] = processors
	}
	return nil
}

func (c *CaptureOb[T]) GetChangefeeds(cfs ...model.ChangeFeedID) ([]*metadata.ChangefeedInfo, []metadata.ChangefeedIdent, error) {
	c.storage.entities.RLock()
	defer c.storage.entities.RUnlock()

	length := len(cfs)
	if length > 0 {
		infos := make([]*metadata.ChangefeedInfo, 0, length)
		ids := make([]metadata.ChangefeedIdent, 0, length)
		for _, id := range cfs {
			infos = append(infos, c.storage.entities.cfs[id])
			ids = append(ids, c.storage.entities.cfids[id])
		}
		return infos, ids, nil
	}

	length = len(c.storage.entities.cfs)
	infos := make([]*metadata.ChangefeedInfo, 0, length)
	ids := make([]metadata.ChangefeedIdent, 0, length)
	for id, info := range c.storage.entities.cfs {
		infos = append(infos, info)
		ids = append(ids, c.storage.entities.cfids[id])
	}
	return infos, ids, nil
}

func (c *CaptureOb[T]) getAllCaptures() []*model.CaptureInfo {
	infos, _ := c.GetCaptures()
	return infos
}

// ControllerOb is an implement for metadata.ControllerObservation.
type ControllerOb[T TxnContext] struct {
	selfInfo *model.CaptureInfo
	checker  LeaderChecker[T]
	client   client[T]

	aliveCaptures struct {
		sync.Mutex
		outgoing     []*model.CaptureInfo
		incoming     []*model.CaptureInfo
		outgoingHash uint64
		incomingHash uint64
	}

	getAllCaptures func() []*model.CaptureInfo
}

func newControllerObservation[T TxnContext](
	checker LeaderChecker[T],
	client client[T],
	selfInfo *model.CaptureInfo,
	getAllCaptures func() []*model.CaptureInfo,
) *ControllerOb[T] {
	return &ControllerOb[T]{
		checker:        checker,
		client:         client,
		selfInfo:       selfInfo,
		getAllCaptures: getAllCaptures,
	}
}

func (c *ControllerOb[T]) run(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			log.Info("controller stops handle alive captures ", zap.String("capture", c.selfInfo.ID), zap.Error(err))
			return err
		case <-ticker.C:
		}

		if err := c.handleAliveCaptures(ctx); err != nil {
			log.Warn("controller handle alive captures fail", zap.String("capture", c.selfInfo.ID), zap.Error(err))
			return err
		}
	}
}

func (c *ControllerOb[T]) handleAliveCaptures(ctx context.Context) error {
	alives := c.getAllCaptures()
	hash := sortAndHashCaptureList(alives)

	c.aliveCaptures.Lock()
	defer c.aliveCaptures.Unlock()
	c.aliveCaptures.incomingHash = hash
	c.aliveCaptures.incoming = alives
	return nil
}

func (c *ControllerOb[T]) CreateChangefeed(cf *metadata.ChangefeedInfo, up *model.UpstreamInfo) (metadata.ChangefeedIdent, error) {
	id := metadata.ChangefeedIdent{ID: cf.ID, UUID: c.storage.epoch.Add(1)}
	return id, nil
}

func (c *ControllerOb[T]) RemoveChangefeed(cf metadata.ChangefeedIdent) error {
	return nil
}

func (c *ControllerOb[T]) RefreshCaptures() (captures []*model.CaptureInfo, changed bool) {
	c.aliveCaptures.Lock()
	defer c.aliveCaptures.Unlock()
	if c.aliveCaptures.outgoingHash != c.aliveCaptures.incomingHash {
		c.aliveCaptures.outgoingHash = c.aliveCaptures.incomingHash
		c.aliveCaptures.outgoing = c.aliveCaptures.incoming
	}
	captures = make([]*model.CaptureInfo, len(c.aliveCaptures.outgoing))
	copy(captures, c.aliveCaptures.outgoing)
	return
}

func (c *ControllerOb[T]) SetOwner(cf metadata.ChangefeedIdent, target metadata.ScheduledChangefeed) error {
}

func (c *ControllerOb[T]) SetProcessors(cf metadata.ChangefeedIdent, workers []metadata.ScheduledChangefeed) error {
}

func (c *ControllerOb[T]) GetChangefeedSchedule(cf metadata.ChangefeedIdent) (s metadata.ChangefeedSchedule, err error) {
	c.storage.RLock()
	defer c.storage.RUnlock()
	s.Owner = c.storage.schedule.owners[cf]
	s.Processors = c.storage.schedule.processors[cf].v
	return
}

func (c *ControllerOb[T]) ScheduleSnapshot() (ss []metadata.ChangefeedSchedule, cs []*model.CaptureInfo, err error) {
	c.storage.RLock()
	ss = make([]metadata.ChangefeedSchedule, 0, len(c.storage.entities.cfids))
	for _, cf := range c.storage.entities.cfids {
		var s metadata.ChangefeedSchedule
		s.Owner = c.storage.schedule.owners[cf]
		s.Processors = c.storage.schedule.processors[cf].v
		ss = append(ss, s)
	}
	c.storage.RUnlock()
	cs = c.getAllCaptures()

	hash := sortAndHashCaptureList(cs)

	c.aliveCaptures.Lock()
	defer c.aliveCaptures.Unlock()
	c.aliveCaptures.outgoingHash = hash
	c.aliveCaptures.outgoing = cs
	return
}

type ownerOb[T TxnContext] struct {
	checker checker[T]
	client  client[T]
	c       *metadata.ChangefeedInfo
	id      metadata.ChangefeedIdent

	processors struct {
		sync.Mutex
		outgoing []metadata.ScheduledChangefeed
		incoming []metadata.ScheduledChangefeed
	}
}

func (o *ownerOb[T]) Self() (*metadata.ChangefeedInfo, metadata.ChangefeedIdent) {
	return o.c, o.id
}

func (o *ownerOb[T]) PauseChangefeed() error {
	o.s.entities.Lock()
	defer o.s.entities.Unlock()
	o.s.Lock()
	defer o.s.Unlock()

	o.s.entities.cfstates[o.id] = 1
	return o.clearSchedule()
}

func (o *ownerOb[T]) ResumeChangefeed() error {
	o.s.entities.Lock()
	defer o.s.entities.Unlock()

	o.s.entities.cfstates[o.id] = 0
	return nil
}

func (o *ownerOb[T]) UpdateChangefeed(info *metadata.ChangefeedInfo) error {
	o.s.entities.Lock()
	defer o.s.entities.Unlock()

	copied := new(metadata.ChangefeedInfo)
	*copied = *info
	copied.Config = info.Config.Clone()
	o.s.entities.cfs[info.ToChangefeedID()] = copied
	return nil
}

func (o *ownerOb[T]) SetChangefeedFinished() error {
	o.s.entities.Lock()
	defer o.s.entities.Unlock()
	o.s.Lock()
	defer o.s.Unlock()

	o.s.entities.cfstates[o.id] = 2
	return o.clearSchedule()
}

func (o *ownerOb[T]) SetChangefeedFailed(err model.RunningError) error {
	return nil
}

func (o *ownerOb[T]) SetChangefeedWarning(warn model.RunningError) error {
	return nil
}

func (o *ownerOb[T]) SetChangefeedPending() error {
	return nil
}

func (o *ownerOb[T]) clearSchedule() error {
	cf := o.id
	if owner, ok := o.s.schedule.owners[cf]; ok {
		owner.State = metadata.SchedRemoving
		if err := o.s.setOwner(cf, owner); err != nil {
			return err
		}
	}
	if processors, ok := o.s.schedule.processors[cf]; ok {
		if err := o.s.setProcessors(cf, processors.v); err != nil {
			return err
		}
	}
	return nil
}

func (o *ownerOb[T]) RefreshProcessors() (captures []metadata.ScheduledChangefeed, changed bool) {
	o.processors.Lock()
	defer o.processors.Unlock()
	return o.processors.outgoing, true
}

func compareByChangefeed(a, b metadata.ScheduledChangefeed) int {
	return a.ChangefeedID.Compare(b.ChangefeedID)
}

func compareByCaptureID(a, b metadata.ScheduledChangefeed) int {
	return strings.Compare(a.CaptureID, b.CaptureID)
}

// sorted `ScheduledChangefeed`s, with a version to simplify diff check.
type sortedScheduledChangefeeds struct {
	version int
	v       []metadata.ScheduledChangefeed
	compare func(a, b metadata.ScheduledChangefeed) int
}

func (s *sortedScheduledChangefeeds) sort() {
	sort.Slice(s.v, func(i, j int) bool { return s.compare(s.v[i], s.v[j]) < 0 })
}

func (s *sortedScheduledChangefeeds) upsert(target metadata.ScheduledChangefeed) {
	i := sort.Search(len(s.v), func(i int) bool { return s.compare(s.v[i], target) >= 0 })
	if i > 0 && i < len(s.v) && s.compare(s.v[i], target) == 0 {
		s.v[i] = target
	} else {
		s.v = append(s.v, target)
		s.sort()
	}
	s.version += 1
}

func (s *sortedScheduledChangefeeds) remove(target metadata.ScheduledChangefeed) {
	i := sort.Search(len(s.v), func(i int) bool { return s.compare(s.v[i], target) >= 0 })
	if i > 0 && i < len(s.v) && s.compare(s.v[i], target) == 0 {
		s.v = append(s.v[:i-1], s.v[i:]...)
		s.version += 1
	}
}

func (s *sortedScheduledChangefeeds) update(target metadata.ScheduledChangefeed) {
	i := sort.Search(len(s.v), func(i int) bool { return s.compare(s.v[i], target) >= 0 })
	if i > 0 && i < len(s.v) && s.compare(s.v[i], target) == 0 {
		s.v[i] = target
		s.version += 1
	}
}

func sortAndHashCaptureList(cs []*model.CaptureInfo) uint64 {
	hasher := fnv.New64()
	sort.Slice(cs, func(i, j int) bool { return strings.Compare(cs[i].ID, cs[j].ID) < 0 })
	for _, info := range cs {
		hasher.Write([]byte(info.ID))
	}
	return hasher.Sum64()
}
