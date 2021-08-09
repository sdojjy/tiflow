//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package writer

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/redo"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// Writer ...
type Writer interface {
	// WriteLog ...
	WriteLog(ctx context.Context, tableID int64, rows []*redo.RowChangedEvent) (offset uint64, err error)
	// SendDDL ...
	SendDDL(ctx context.Context, ddl *redo.DDLEvent) error
	// 	FlushLog ...
	FlushLog(ctx context.Context, tableID int64, ts uint64) error
	// EmitCheckpointTs ...
	EmitCheckpointTs(ctx context.Context, ts uint64) error
	// EmitResolvedTs ...
	EmitResolvedTs(ctx context.Context, ts uint64) error
	// GetCurrentOffset ...
	GetCurrentOffset(ctx context.Context, tableIDs []int64) (offsets map[int64]uint64, err error)
}

const (
	defaultMetaFileName   = "meta"
	defaultRowLogFileName = "row"
	defaultDDLLogFileName = "ddl"

	defaultGCIntervalInMins = 5
)

const (
	// dirty defines the state of the log meta
	dirty int32 = 0
	// clean defines the state of the log meta
	clean int32 = 1
)

var (
	initOnce  sync.Once
	logWriter *LogWriter
)

// LogWriterConfig is the configuration used by a writer.
type LogWriterConfig struct {
	dir          string
	changeFeedID string
	startTs      uint64
	CreateTime   time.Time
	// maxLogSize is the maximum size of log in megabyte, defaults to defaultMaxLogSize.
	maxLogSize         int64
	flushIntervalInSec int64
	s3Storage          bool
	s3URI              *url.URL
}

// LogWriter ...
type LogWriter struct {
	cfg       *LogWriterConfig
	rowWriter *writer
	ddlWriter *writer
	meta      *redo.LogMeta
	metaLock  sync.RWMutex
	dirtyMeta int32
}

// NewLogWriter creates a writer instance.
func NewLogWriter(ctx context.Context, cfg *LogWriterConfig) *LogWriter {
	initOnce.Do(func() {
		rowCfg := &writerConfig{
			dir:                cfg.dir,
			changeFeedID:       cfg.changeFeedID,
			fileName:           defaultRowLogFileName,
			startTs:            cfg.startTs,
			CreateTime:         cfg.CreateTime,
			maxLogSize:         cfg.maxLogSize,
			flushIntervalInSec: cfg.flushIntervalInSec,
		}
		ddlCfg := &writerConfig{
			dir:                cfg.dir,
			changeFeedID:       cfg.changeFeedID,
			fileName:           defaultDDLLogFileName,
			startTs:            cfg.startTs,
			CreateTime:         cfg.CreateTime,
			maxLogSize:         cfg.maxLogSize,
			flushIntervalInSec: cfg.flushIntervalInSec,
		}
		logWriter = &LogWriter{
			rowWriter: newWriter(ctx, rowCfg),
			ddlWriter: newWriter(ctx, ddlCfg),
			meta:      &redo.LogMeta{Offsets: map[int64]uint64{}},
		}

		go logWriter.runGC(ctx)
	})
	return logWriter
}

func (l *LogWriter) runGC(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(defaultGCIntervalInMins) * time.Minute)
	defer ticker.Stop()

	for {
		if l.isStopped() {
			return
		}

		select {
		case <-ctx.Done():
			log.Info("runGC got canceled", zap.Error(ctx.Err()))
			return
		case <-ticker.C:
			err := l.gc()
			if err != nil {
				log.Error("redo log gc error", zap.Error(err))
			}
		}
	}
}

func (l *LogWriter) gc() error {
	var err error
	err = multierr.Append(err, l.rowWriter.gc(atomic.LoadUint64(&l.meta.CheckPointTs)))
	err = multierr.Append(err, l.ddlWriter.gc(atomic.LoadUint64(&l.meta.CheckPointTs)))
	return err
}

// WriteLog implement WriteLog api
func (l *LogWriter) WriteLog(ctx context.Context, tableID int64, rows []*redo.RowChangedEvent) (uint64, error) {
	select {
	case <-ctx.Done():
		return 0, errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return 0, cerror.ErrRedoWriterStopped
	}
	if len(rows) == 0 {
		return 0, nil
	}

	var maxCommitTs uint64
	for _, r := range rows {
		data, err := r.MarshalMsg(nil)
		if err != nil {
			return maxCommitTs, cerror.WrapError(cerror.ErrMarshalFailed, err)
		}

		_, err = l.rowWriter.Write(data)
		if err != nil {
			return maxCommitTs, err
		}
		maxCommitTs = l.setMaxCommitTs(tableID, r.CommitTs)
	}
	// TODO: get a better name pattern, used in file name for search
	if maxCommitTs > l.rowWriter.commitTS.Load() {
		l.rowWriter.commitTS.Store(maxCommitTs)
	}
	return maxCommitTs, nil
}

func (l *LogWriter) setMaxCommitTs(tableID int64, commitTs uint64) uint64 {
	l.metaLock.Lock()
	defer l.metaLock.Unlock()

	if v, ok := l.meta.Offsets[tableID]; ok {
		if v < commitTs {
			l.meta.Offsets[tableID] = commitTs
			atomic.StoreInt32(&l.dirtyMeta, dirty)
		}
	} else {
		l.meta.Offsets[tableID] = commitTs
		atomic.StoreInt32(&l.dirtyMeta, dirty)
	}

	return l.meta.Offsets[tableID]
}

// SendDDL implement SendDDL api
func (l *LogWriter) SendDDL(ctx context.Context, ddl *redo.DDLEvent) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped
	}

	data, err := ddl.MarshalMsg(nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	_, err = l.ddlWriter.Write(data)
	return err
}

// FlushLog implement FlushLog api
func (l *LogWriter) FlushLog(ctx context.Context, tableID int64, ts uint64) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped
	}

	if err := l.flush(); err != nil {
		return err
	}
	l.setMaxCommitTs(tableID, ts)
	return nil
}

// EmitCheckpointTs implement EmitCheckpointTs api
func (l *LogWriter) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped
	}
	atomic.StoreUint64(&l.meta.CheckPointTs, ts)
	atomic.StoreInt32(&l.dirtyMeta, dirty)
	return l.flushLogMeta()
}

// EmitResolvedTs implement EmitResolvedTs api
func (l *LogWriter) EmitResolvedTs(ctx context.Context, ts uint64) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return cerror.ErrRedoWriterStopped
	}
	atomic.StoreUint64(&l.meta.ResolvedTs, ts)
	atomic.StoreInt32(&l.dirtyMeta, dirty)

	return l.flushLogMeta()
}

// GetCurrentOffset implement GetCurrentOffset api
func (l *LogWriter) GetCurrentOffset(ctx context.Context, tableIDs []int64) (map[int64]uint64, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	default:
	}

	if len(tableIDs) == 0 {
		return nil, nil
	}

	l.metaLock.RLock()
	defer l.metaLock.RUnlock()

	ret := map[int64]uint64{}
	for i := 0; i < len(tableIDs); i++ {
		id := tableIDs[i]
		if v, ok := l.meta.Offsets[id]; ok {
			ret[id] = v
		}
	}

	return ret, nil
}

// Close implements Writer.Close.
func (l *LogWriter) Close() error {
	var err error
	err = multierr.Append(err, l.rowWriter.Close())
	err = multierr.Append(err, l.ddlWriter.Close())
	return err
}

// flush flushes all the buffered data to the disk.
func (l *LogWriter) flush() error {
	err1 := l.flushLogMeta()
	err2 := l.ddlWriter.flush()
	err3 := l.rowWriter.flush()

	err := multierr.Append(err1, err2)
	err = multierr.Append(err, err3)
	return err
}

func (l *LogWriter) isStopped() bool {
	return l.ddlWriter.state.Load() == stopped || l.rowWriter.state.Load() == stopped
}

func (l *LogWriter) getMetafileName() string {
	return fmt.Sprintf("%s_%d_%s", l.cfg.changeFeedID, l.cfg.CreateTime.Unix(), defaultMetaFileName)
}

func (l *LogWriter) flushLogMeta() error {
	if atomic.LoadInt32(&l.dirtyMeta) == clean {
		return nil
	}

	l.metaLock.RLock()
	defer l.metaLock.RUnlock()

	data, err := l.meta.MarshalMsg(nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	tmpFileName := l.getMetafileName() + ".tmp"
	tmpFile, err := os.OpenFile(tmpFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, defaultFileMode)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	_, err = tmpFile.Write(data)
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}
	err = tmpFile.Sync()
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	err = os.Rename(tmpFileName, l.getMetafileName())
	if err != nil {
		return cerror.WrapError(cerror.ErrRedoFileOp, err)
	}

	atomic.StoreInt32(&l.dirtyMeta, dirty)
	return nil
}
