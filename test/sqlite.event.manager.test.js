import test from 'node:test';
import assert from 'node:assert/strict';

import { sqliteTable, integer, text } from 'drizzle-orm/sqlite-core';
import { getTableUniqueName } from 'drizzle-orm';
import { EventPriority } from '@oglofus/event-manager';

import {
  SQLiteEventManager,
} from '../dist/sqlite/index.js';

// Minimal in-memory mock for BaseSQLiteDatabase used by SQLiteEventManager.
class MockSQLiteDatabase {
  constructor() {
    this._store = new Map(); // key: tableUniqueName => Array of rows
  }

  _getTableKey(table) {
    return getTableUniqueName(table);
  }

  _getRows(table) {
    const key = this._getTableKey(table);
    if (!this._store.has(key)) this._store.set(key, []);
    return this._store.get(key);
  }

  select(/* fields */) {
    const self = this;
    return {
      from(table) {
        return {
          where(/* condition */) {
            return {
              async get() {
                const rows = self._getRows(table);
                return rows.length ? { ...rows[0] } : undefined;
              }
            };
          }
        };
      }
    };
  }

  insert(table) {
    const self = this;
    return {
      values(data) {
        const rows = self._getRows(table);
        const row = { ...data };
        rows.push(row);
        return {
          async returning() {
            return [{ ...row }];
          },
          async execute() {
            // no-op, already inserted
          }
        };
      }
    };
  }

  update(table) {
    const self = this;
    const ctx = { data: undefined };
    return {
      set(data) {
        ctx.data = data;
        return {
          where(/* condition */) {
            return {
              async returning() {
                const rows = self._getRows(table);
                if (!rows.length) return [];
                Object.assign(rows[0], ctx.data);
                return [{ ...rows[0] }];
              },
              async execute() {
                const rows = self._getRows(table);
                if (!rows.length) return;
                Object.assign(rows[0], ctx.data);
              }
            };
          }
        };
      }
    };
  }

  delete(table) {
    const self = this;
    return {
      where(/* condition */) {
        return {
          async execute() {
            const rows = self._getRows(table);
            rows.splice(0, rows.length);
          }
        };
      }
    };
  }
}

// Define a simple table schema using drizzle helpers so getTableUniqueName works.
const users = sqliteTable('users', {
  id: integer('id').primaryKey(),
  name: text('name'),
  // even though defined as text, we will store an object for testing deep merge behavior.
  meta: text('meta'),
});

function createManager(db, config) {
  return new SQLiteEventManager(db, config);
}

// Tests

test('insert: pre and post events run, data can be modified in pre-insert', async () => {
  const db = new MockSQLiteDatabase();
  const manager = createManager(db);
  const calls = [];

  manager.put(users, 'pre-insert', (event) => {
    calls.push('pre');
    // mutate incoming data
    event.data.name = 'Bob';
  }, EventPriority.HIGH);

  manager.put(users, 'post-insert', (event) => {
    calls.push('post');
    assert.equal(event.row.name, 'Bob');
  }, EventPriority.NORMAL);

  const res = await manager.insert(users, 'id', { id: 1, name: 'Alice', meta: { a: 1 } });
  assert.equal(res.type, 'success');
  assert.equal(res.data.name, 'Bob');
  assert.deepEqual(calls, ['pre', 'post']);
});


test('insert: pre-insert cancellation prevents insert', async () => {
  const db = new MockSQLiteDatabase();
  const manager = createManager(db);

  manager.put(users, 'pre-insert', (event) => {
    event.cancel('stop');
  }, EventPriority.NORMAL);

  const res = await manager.insert(users, 'id', { id: 1, name: 'Alice' });
  assert.equal(res.type, 'error');
  assert.equal(res.message, 'stop');

  // Ensure no row was inserted
  const row = await db.select({}).from(users).where({}).get();
  assert.equal(row, undefined);
});


test('insert: post-insert cancellation triggers rollback when enabled', async () => {
  const db = new MockSQLiteDatabase();
  const manager = createManager(db, { rollback_on_cancel: true });

  manager.put(users, 'post-insert', (event) => {
    event.cancel('rollback');
  }, EventPriority.NORMAL);

  const res = await manager.insert(users, 'id', { id: 1, name: 'Alice' });
  assert.equal(res.type, 'error');
  assert.equal(res.message, 'rollback');

  // Row should be rolled back (deleted)
  const row = await db.select({}).from(users).where({}).get();
  assert.equal(row, undefined);
});


test('update: merges objects when enabled and respects post-update rollback', async () => {
  const db = new MockSQLiteDatabase();
  const manager = createManager(db, { merge_objects: true, rollback_on_cancel: true });

  // seed a row
  await db.insert(users).values({ id: 1, name: 'Alice', meta: { a: 1, arr: [1] } }).execute();

  let preSeenOld;
  manager.put(users, 'pre-update', (event) => {
    // Ensure old row is available
    preSeenOld = event.row;
    // mutate update payload
    event.data.name = 'Alice2';
    event.data.meta = { b: 2, arr: [1, 2] };
  });

  let postSeenOld, postSeenRow;
  manager.put(users, 'post-update', (event) => {
    postSeenOld = event.old_row;
    postSeenRow = event.row;
  });

  const res = await manager.update(users, 'id', 1, { /* will be overwritten by pre handler */ });
  assert.equal(res.type, 'success');

  // pre handler saw the original row
  assert.deepEqual(preSeenOld, { id: 1, name: 'Alice', meta: { a: 1, arr: [1] } });

  // Because SQLiteEventManager uses deepMerge() without passing config.array_strategy,
  // arrays use the default 'replace' strategy. So arr should equal [1,2], not union.
  assert.deepEqual(res.data, { id: 1, name: 'Alice2', meta: { a: 1, b: 2, arr: [1, 2] } });
  assert.deepEqual(postSeenOld, { id: 1, name: 'Alice', meta: { a: 1, arr: [1] } });
  assert.deepEqual(postSeenRow, res.data);

  // Now test post-update cancellation triggers rollback to old row
  manager.put(users, 'post-update', (event) => {
    event.cancel('undo');
  }, EventPriority.HIGHEST);

  const res2 = await manager.update(users, 'id', 1, { name: 'Changed', meta: { a: 999 } });
  assert.equal(res2.type, 'error');
  assert.equal(res2.message, 'undo');

  const rowAfterRollback = await db.select({}).from(users).where({}).get();
  assert.deepEqual(rowAfterRollback, { id: 1, name: 'Alice2', meta: { a: 1, b: 2, arr: [1, 2] } });
});


test('delete: pre and post events, cancellation and rollback', async () => {
  const db = new MockSQLiteDatabase();
  const manager = createManager(db, { rollback_on_cancel: true });

  await db.insert(users).values({ id: 1, name: 'Alice' }).execute();

  // pre-delete cancellation blocks delete
  manager.put(users, 'pre-delete', (event) => {
    event.cancel('nope');
  });
  const res1 = await manager.delete(users, 'id', 1);
  assert.equal(res1.type, 'error');
  assert.equal(res1.message, 'nope');
  assert.equal((await db.select({}).from(users).where({}).get()).name, 'Alice');

  // allow delete, but cancel in post-delete and rollback
  const manager2 = createManager(db, { rollback_on_cancel: true });
  manager2.put(users, 'post-delete', (event) => {
    event.cancel('restore');
  });
  const res2 = await manager2.delete(users, 'id', 1);
  assert.equal(res2.type, 'error');
  assert.equal(res2.message, 'restore');
  // row should be restored
  const row = await db.select({}).from(users).where({}).get();
  assert.equal(row.name, 'Alice');
});


test('handler priority affects execution order per event type', async () => {
  const db = new MockSQLiteDatabase();
  const manager = createManager(db);
  const order = [];

  manager.put(users, 'pre-insert', () => order.push('low'), EventPriority.LOW);
  manager.put(users, 'pre-insert', () => order.push('high'), EventPriority.HIGH);
  manager.put(users, 'post-insert', () => order.push('post-low'), EventPriority.LOW);
  manager.put(users, 'post-insert', () => order.push('post-high'), EventPriority.HIGH);

  const res = await manager.insert(users, 'id', { id: 1, name: 'Alice' });
  assert.equal(res.type, 'success');

  // HIGHEST(1) < HIGH(2) < NORMAL(3) < LOW(4). So HIGH before LOW.
  assert.deepEqual(order, ['high', 'low', 'post-high', 'post-low']);
});
