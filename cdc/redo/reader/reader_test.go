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

package reader

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/redo/common"
	"github.com/pingcap/ticdc/cdc/redo/writer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewLogReader(t *testing.T) {
	_, err := NewLogReader(context.Background(), nil)
	require.NotNil(t, err)

	_, err = NewLogReader(context.Background(), &LogReaderConfig{})
	require.Nil(t, err)
}

func TestLogReader_ResetReader(t *testing.T) {
	dir, err := ioutil.TempDir("", "redo-ResetReader")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &writer.FileWriterConfig{
		MaxLogSize: 100000,
		Dir:        dir,
	}
	fileName := fmt.Sprintf("%s_%s_%d_%s_%d%s", "cp", "test-cf100", time.Now().Unix(), common.DefaultDDLLogFileType, 100, common.LogEXT)
	w := writer.NewWriter(ctx, cfg, writer.WithLogFileName(func() string {
		return fileName
	}))
	log := &model.RedoLog{
		Row: &model.RedoRowChangedEvent{Row: &model.RowChangedEvent{CommitTs: 11}},
	}
	data, err := log.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = w.Write(data)
	require.Nil(t, err)
	w.Close()

	path := filepath.Join(dir, fileName)
	f, err := os.Open(path)
	require.Nil(t, err)

	fileName = fmt.Sprintf("%s_%s_%d_%s_%d%s", "cp", "test-cf10", time.Now().Unix(), common.DefaultRowLogFileType, 10, common.LogEXT)
	w = writer.NewWriter(ctx, cfg, writer.WithLogFileName(func() string {
		return fileName
	}))
	log = &model.RedoLog{
		Row: &model.RedoRowChangedEvent{Row: &model.RowChangedEvent{CommitTs: 11}},
	}
	data, err = log.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = w.Write(data)
	require.Nil(t, err)
	w.Close()
	path = filepath.Join(dir, fileName)
	f1, err := os.Open(path)
	require.Nil(t, err)

	type arg struct {
		ctx                      context.Context
		startTs, endTs           uint64
		resolvedTs, checkPointTs uint64
	}
	tests := []struct {
		name                   string
		args                   arg
		readerErr              error
		wantErr                bool
		wantStartTs, wantEndTs uint64
		rowFleName             string
		ddlFleName             string
	}{
		{
			name: "happy",
			args: arg{
				ctx:          context.Background(),
				startTs:      1,
				endTs:        101,
				checkPointTs: 0,
				resolvedTs:   200,
			},
			wantStartTs: 1,
			wantEndTs:   101,
			rowFleName:  f1.Name(),
			ddlFleName:  f.Name(),
		},
		{
			name: "context cancel",
			args: arg{
				ctx:          context.Background(),
				startTs:      1,
				endTs:        101,
				checkPointTs: 0,
				resolvedTs:   200,
			},
			wantErr: true,
		},
		{
			name: "invalid ts",
			args: arg{
				ctx:          context.Background(),
				startTs:      1,
				endTs:        0,
				checkPointTs: 0,
				resolvedTs:   200,
			},
			wantErr: true,
		},
		{
			name: "invalid ts",
			args: arg{
				ctx:          context.Background(),
				startTs:      201,
				endTs:        10,
				checkPointTs: 0,
				resolvedTs:   200,
			},
			wantErr: true,
		},
		{
			name: "reader close err",
			args: arg{
				ctx:          context.Background(),
				startTs:      1,
				endTs:        10,
				checkPointTs: 0,
				resolvedTs:   200,
			},
			wantErr:   true,
			readerErr: errors.New("err"),
		},
	}

	for _, tt := range tests {
		mockReader := &mockFileReader{}
		mockReader.On("Close").Return(tt.readerErr)
		r := &LogReader{
			cfg:       &LogReaderConfig{Dir: dir},
			rowReader: []fileReader{mockReader},
			ddlReader: []fileReader{mockReader},
			meta:      &common.LogMeta{CheckPointTs: tt.args.checkPointTs, ResolvedTs: tt.args.resolvedTs},
		}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx
		} else {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tt.args.ctx = ctx
		}
		err := r.ResetReader(tt.args.ctx, tt.args.startTs, tt.args.endTs)
		if tt.wantErr {
			require.NotNil(t, err, tt.name)
		} else {
			require.Nil(t, err, tt.name)
			mockReader.AssertNumberOfCalls(t, "Close", 2)
			require.Equal(t, tt.rowFleName+common.SortLogEXT, r.rowReader[0].(*reader).fileName, tt.name)
			require.Equal(t, tt.ddlFleName+common.SortLogEXT, r.ddlReader[0].(*reader).fileName, tt.name)
			require.Equal(t, tt.wantStartTs, r.cfg.startTs, tt.name)
			require.Equal(t, tt.wantEndTs, r.cfg.endTs, tt.name)

		}
	}
}

func TestLogReader_ReadMeta(t *testing.T) {
	dir, err := ioutil.TempDir("", "redo-ReadMeta")
	require.Nil(t, err)
	defer os.RemoveAll(dir)

	fileName := fmt.Sprintf("%s_%s_%d_%s%s", "cp", "test-changefeed", time.Now().Unix(), common.DefaultMetaFileType, common.MetaEXT)
	path := filepath.Join(dir, fileName)
	f, err := os.Create(path)
	require.Nil(t, err)
	meta := &common.LogMeta{
		CheckPointTs: 11,
		ResolvedTs:   22,
	}
	data, err := meta.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)

	fileName = fmt.Sprintf("%s_%s_%d_%s%s", "cp1", "test-changefeed", time.Now().Unix(), common.DefaultMetaFileType, common.MetaEXT)
	path = filepath.Join(dir, fileName)
	f, err = os.Create(path)
	require.Nil(t, err)
	meta = &common.LogMeta{
		CheckPointTs: 111,
		ResolvedTs:   21,
	}
	data, err = meta.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)

	dir1, err := ioutil.TempDir("", "redo-NoReadMeta")
	require.Nil(t, err)
	defer os.RemoveAll(dir1)

	tests := []struct {
		name                             string
		dir                              string
		wantCheckPointTs, wantResolvedTs uint64
		wantErr                          bool
	}{
		{
			name:             "happy",
			dir:              dir,
			wantCheckPointTs: meta.CheckPointTs,
			wantResolvedTs:   meta.ResolvedTs,
		},
		{
			name:    "no meta file",
			dir:     dir1,
			wantErr: true,
		},
		{
			name:    "wrong dir",
			dir:     "xxx",
			wantErr: true,
		},
		{
			name:             "context cancel",
			dir:              dir,
			wantCheckPointTs: meta.CheckPointTs,
			wantResolvedTs:   meta.ResolvedTs,
			wantErr:          true,
		},
	}
	for _, tt := range tests {
		l := &LogReader{
			cfg: &LogReaderConfig{
				Dir: tt.dir,
			},
		}
		ctx := context.Background()
		if tt.name == "context cancel" {
			ctx1, cancel := context.WithCancel(context.Background())
			cancel()
			ctx = ctx1
		}
		cts, rts, err := l.ReadMeta(ctx)
		if tt.wantErr {
			require.NotNil(t, err, tt.name)
		} else {
			require.Nil(t, err, tt.name)
			require.Equal(t, tt.wantCheckPointTs, cts, tt.name)
			require.Equal(t, tt.wantResolvedTs, rts, tt.name)
		}
	}
}

func TestLogReader_ReadNextLog(t *testing.T) {
	type arg struct {
		ctx    context.Context
		maxNum uint64
	}
	tests := []struct {
		name       string
		args       arg
		wantErr    bool
		readerErr  error
		readerErr1 error
		readerRet  *model.RedoLog
		readerRet1 *model.RedoLog
	}{
		{
			name: "happy",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				Row: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 5,
						RowID:    1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				Row: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 6,
						RowID:    2,
					},
				},
			},
		},
		{
			name: "context cancel",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				Row: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 5,
						RowID:    1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				Row: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 6,
						RowID:    2,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "happy1",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				Row: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 1,
						RowID:    1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				Row: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 6,
						RowID:    2,
					},
				},
			},
		},
		{
			name: "io.EOF err",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				Row: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 5,
						RowID:    1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				Row: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 6,
						RowID:    2,
					},
				},
			},
			readerErr: io.EOF,
		},
		{
			name: "err",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				Row: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 5,
						RowID:    1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				Row: &model.RedoRowChangedEvent{
					Row: &model.RowChangedEvent{
						CommitTs: 6,
						RowID:    2,
					},
				},
			},
			readerErr:  errors.New("xx"),
			readerErr1: errors.New("xx"),
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		mockReader := &mockFileReader{}
		mockReader.On("Read", mock.Anything).Return(tt.readerErr).Run(func(args mock.Arguments) {
			arg := args.Get(0).(*model.RedoLog)
			arg.Row = tt.readerRet.Row
			arg.Type = model.RedoLogTypeRow
		})
		mockReader1 := &mockFileReader{}
		mockReader1.On("Read", mock.Anything).Return(tt.readerErr1).Run(func(args mock.Arguments) {
			arg := args.Get(0).(*model.RedoLog)
			arg.Row = tt.readerRet1.Row
			arg.Type = model.RedoLogTypeRow
		})

		l := &LogReader{
			rowReader: []fileReader{mockReader1, mockReader},
			rowHeap:   logHeap{},
			cfg: &LogReaderConfig{
				startTs: 1,
				endTs:   10,
			},
		}
		if tt.name == "context cancel" {
			ctx1, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx1
		}
		ret, err := l.ReadNextLog(tt.args.ctx, tt.args.maxNum)
		if tt.wantErr {
			require.NotNil(t, err, tt.name)
			require.Equal(t, 0, len(ret), tt.name)
		} else {
			require.Nil(t, err, tt.name)
			require.EqualValues(t, tt.args.maxNum, len(ret), tt.name)
			for i := 0; i < int(tt.args.maxNum); i++ {
				if tt.name == "io.EOF err" {
					require.Equal(t, ret[i].Row.CommitTs, tt.readerRet1.Row.Row.CommitTs, tt.name)
					continue
				}
				if tt.name == "happy1" {
					require.Equal(t, ret[i].Row.CommitTs, tt.readerRet1.Row.Row.CommitTs, tt.name)
					continue
				}
				require.Equal(t, ret[i].Row.CommitTs, tt.readerRet.Row.Row.CommitTs, tt.name)
			}

		}
	}
}

func TestLogReader_ReadNexDDL(t *testing.T) {
	type arg struct {
		ctx    context.Context
		maxNum uint64
	}
	tests := []struct {
		name       string
		args       arg
		wantErr    bool
		readerErr  error
		readerErr1 error
		readerRet  *model.RedoLog
		readerRet1 *model.RedoLog
	}{
		{
			name: "happy",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				DDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 5,
					},
				},
			},
			readerRet1: &model.RedoLog{
				DDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 6,
					},
				},
			},
		},
		{
			name: "context cancel",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				DDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 5,
					},
				},
			},
			readerRet1: &model.RedoLog{
				DDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 6,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "happy1",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				DDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 1,
					},
				},
			},
			readerRet1: &model.RedoLog{
				DDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 6,
					},
				},
			},
		},
		{
			name: "io.EOF err",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				DDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 5,
					},
				},
			},
			readerRet1: &model.RedoLog{
				DDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 6,
					},
				},
			},
			readerErr: io.EOF,
		},
		{
			name: "err",
			args: arg{
				ctx:    context.Background(),
				maxNum: 3,
			},
			readerRet: &model.RedoLog{
				DDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 5,
					},
				},
			},
			readerRet1: &model.RedoLog{
				DDL: &model.RedoDDLEvent{
					DDL: &model.DDLEvent{
						CommitTs: 6,
					},
				},
			},
			readerErr:  errors.New("xx"),
			readerErr1: errors.New("xx"),
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		mockReader := &mockFileReader{}
		mockReader.On("Read", mock.Anything).Return(tt.readerErr).Run(func(args mock.Arguments) {
			arg := args.Get(0).(*model.RedoLog)
			arg.DDL = tt.readerRet.DDL
			arg.Type = model.RedoLogTypeDDL
		})
		mockReader1 := &mockFileReader{}
		mockReader1.On("Read", mock.Anything).Return(tt.readerErr1).Run(func(args mock.Arguments) {
			arg := args.Get(0).(*model.RedoLog)
			arg.DDL = tt.readerRet1.DDL
			arg.Type = model.RedoLogTypeDDL
		})

		l := &LogReader{
			ddlReader: []fileReader{mockReader1, mockReader},
			ddlHeap:   logHeap{},
			cfg: &LogReaderConfig{
				startTs: 1,
				endTs:   10,
			},
		}
		if tt.name == "context cancel" {
			ctx1, cancel := context.WithCancel(context.Background())
			cancel()
			tt.args.ctx = ctx1
		}
		ret, err := l.ReadNextDDL(tt.args.ctx, tt.args.maxNum)
		if tt.wantErr {
			require.NotNil(t, err, tt.name)
			require.Equal(t, 0, len(ret), tt.name)
		} else {
			require.Nil(t, err, tt.name)
			require.EqualValues(t, tt.args.maxNum, len(ret), tt.name)
			for i := 0; i < int(tt.args.maxNum); i++ {
				if tt.name == "io.EOF err" {
					require.Equal(t, ret[i].DDL.CommitTs, tt.readerRet1.DDL.DDL.CommitTs, tt.name)
					continue
				}
				if tt.name == "happy1" {
					require.Equal(t, ret[i].DDL.CommitTs, tt.readerRet1.DDL.DDL.CommitTs, tt.name)
					continue
				}
				require.Equal(t, ret[i].DDL.CommitTs, tt.readerRet.DDL.DDL.CommitTs, tt.name)
			}
		}
	}
}

func TestLogReader_Close(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
		err     error
	}{
		{
			name: "happy",
		},
		{
			name:    "err",
			err:     errors.New("xx"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		mockReader := &mockFileReader{}
		mockReader.On("Close").Return(tt.err)
		l := &LogReader{
			rowReader: []fileReader{mockReader},
			ddlReader: []fileReader{mockReader},
		}
		err := l.Close()
		mockReader.AssertNumberOfCalls(t, "Close", 2)
		if tt.wantErr {
			require.NotNil(t, err, tt.name)
		} else {
			require.Nil(t, err, tt.name)
		}
	}
}
