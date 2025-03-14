// Copyright 2020 PingCAP, Inc.
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

package pessimism

import (
	"github.com/pingcap/check"
)

type testLockKeeper struct{}

var _ = check.Suite(&testLockKeeper{})

func (t *testLockKeeper) TestLockKeeper(c *check.C) {
	var (
		lk      = NewLockKeeper()
		schema  = "foo"
		table   = "bar"
		DDLs    = []string{"ALTER TABLE bar ADD COLUMN c1 INT"}
		task1   = "task1"
		task2   = "task2"
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		info11  = NewInfo(task1, source1, schema, table, DDLs)
		info12  = NewInfo(task1, source2, schema, table, DDLs)
		info21  = NewInfo(task2, source1, schema, table, DDLs)
	)

	// lock with 2 sources.
	lockID1, synced, remain, err := lk.TrySync(info11, []string{source1, source2})
	c.Assert(err, check.IsNil)
	c.Assert(lockID1, check.Equals, "task1-`foo`.`bar`")
	c.Assert(synced, check.IsFalse)
	c.Assert(remain, check.Equals, 1)
	lockID1, synced, remain, err = lk.TrySync(info12, []string{source1, source2})
	c.Assert(err, check.IsNil)
	c.Assert(lockID1, check.Equals, "task1-`foo`.`bar`")
	c.Assert(synced, check.IsTrue)
	c.Assert(remain, check.Equals, 0)

	// lock with only 1 source.
	lockID2, synced, remain, err := lk.TrySync(info21, []string{source1})
	c.Assert(err, check.IsNil)
	c.Assert(lockID2, check.Equals, "task2-`foo`.`bar`")
	c.Assert(synced, check.IsTrue)
	c.Assert(remain, check.Equals, 0)

	// find lock.
	lock1 := lk.FindLock(lockID1)
	c.Assert(lock1, check.NotNil)
	c.Assert(lock1.ID, check.Equals, lockID1)
	lock2 := lk.FindLock(lockID2)
	c.Assert(lock2, check.NotNil)
	c.Assert(lock2.ID, check.Equals, lockID2)
	lockIDNotExists := "lock-not-exists"
	c.Assert(lk.FindLock(lockIDNotExists), check.IsNil)

	// all locks.
	locks := lk.Locks()
	c.Assert(locks, check.HasLen, 2)
	c.Assert(locks[lockID1], check.Equals, lock1) // compare pointer
	c.Assert(locks[lockID2], check.Equals, lock2)

	// remove lock.
	c.Assert(lk.RemoveLock(lockID1), check.IsTrue)
	c.Assert(lk.RemoveLock(lockIDNotExists), check.IsFalse)
	c.Assert(lk.Locks(), check.HasLen, 1)

	// clear locks.
	lk.Clear()

	// no locks exist.
	c.Assert(lk.Locks(), check.HasLen, 0)
}
