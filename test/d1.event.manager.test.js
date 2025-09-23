import test from 'node:test';
import assert from 'node:assert/strict';

import { integer, sqliteTable, text } from 'drizzle-orm/sqlite-core';
import { getTableUniqueName } from 'drizzle-orm';
import { EventPriority } from '@oglofus/event-manager';

import { D1EventManager } from '../dist/d1/index.js';

// Minimal in-memory mock for AnyD1Database used by D1EventManager.
class MockD1Database {
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
				return {
					// For D1.batch(), we return a thunk that when executed performs the insert
					async returning() {
						rows.push(row);
						return [{ ...row }];
					},
					async execute() {
						rows.push(row);
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

	// D1 specific method used by D1EventManager
	async batch(requests) {
		// Each request is expected to be a Promise-like (returning()) that resolves to an array of rows
		const out = [];
		for (const req of requests) {
			const res = await req; // our returning() returns a promise
			if (Array.isArray(res)) out.push(...res);
			else out.push(res);
		}
		return out;
	}
}

// Define a simple table schema using drizzle helpers so getTableUniqueName works.
const users = sqliteTable('users', {
	id: integer('id').primaryKey(),
	name: text('name'),
	meta: text('meta')
});

function createManager(db, config) {
	return new D1EventManager(db, config);
}

// Tests

test('D1 insertBatch: pre and post events run for each row and data can be modified', async () => {
	const db = new MockD1Database();
	const manager = createManager(db);
	const calls = [];

	manager.put(
		users,
		'pre-insert',
		(event) => {
			calls.push(`pre-${event.data.id}`);
			// mutate incoming data
			event.data.name = `User-${event.data.id}`;
		},
		EventPriority.HIGH
	);

	manager.put(
		users,
		'post-insert',
		(event) => {
			calls.push(`post-${event.row.id}`);
			assert.equal(event.row.name, `User-${event.row.id}`);
		},
		EventPriority.NORMAL
	);

	const res = await manager.insertBatch(users, 'id', [
		{ id: 1, name: 'Alice' },
		{ id: 2, name: 'Bob' }
	]);
	assert.equal(res.type, 'success');
	assert.equal(res.data.length, 2);
	assert.deepEqual(
		res.data.map((r) => r.name),
		['User-1', 'User-2']
	);
	assert.deepEqual(calls, ['pre-1', 'pre-2', 'post-1', 'post-2']);
});

test('D1 insertBatch: pre-insert cancellation prevents entire batch', async () => {
	const db = new MockD1Database();
	const manager = createManager(db);

	manager.put(
		users,
		'pre-insert',
		(event) => {
			if (event.data.id === 2) event.cancel('stop-2');
		},
		EventPriority.NORMAL
	);

	const res = await manager.insertBatch(users, 'id', [
		{ id: 1, name: 'Alice' },
		{ id: 2, name: 'Bob' }
	]);
	assert.equal(res.type, 'error');
	assert.equal(res.message, 'stop-2');

	// Ensure no rows were inserted
	const row = await db.select({}).from(users).where({}).get();
	assert.equal(row, undefined);
});

test('D1 insertBatch: post-insert cancellation triggers rollback of all inserted rows', async () => {
	const db = new MockD1Database();
	const manager = createManager(db);

	manager.put(
		users,
		'post-insert',
		(event) => {
			if (event.row.id === 2) event.cancel('rollback-2');
		},
		EventPriority.NORMAL
	);

	const res = await manager.insertBatch(users, 'id', [
		{ id: 1, name: 'Alice' },
		{ id: 2, name: 'Bob' },
		{ id: 3, name: 'Carol' }
	]);
	assert.equal(res.type, 'error');
	assert.equal(res.message, 'rollback-2');

	// All inserted rows should be rolled back (deleted)
	const row = await db.select({}).from(users).where({}).get();
	assert.equal(row, undefined);
});

test('D1 handler priority affects execution order for insert events', async () => {
	const db = new MockD1Database();
	const manager = createManager(db);
	const order = [];

	manager.put(users, 'pre-insert', () => order.push('low'), EventPriority.LOW);
	manager.put(users, 'pre-insert', () => order.push('high'), EventPriority.HIGH);
	manager.put(users, 'post-insert', () => order.push('post-low'), EventPriority.LOW);
	manager.put(users, 'post-insert', () => order.push('post-high'), EventPriority.HIGH);

	const res = await manager.insertBatch(users, 'id', [{ id: 1, name: 'Alice' }]);
	assert.equal(res.type, 'success');

	// HIGHEST(1) < HIGH(2) < NORMAL(3) < LOW(4). So HIGH before LOW.
	assert.deepEqual(order, ['high', 'low', 'post-high', 'post-low']);
});
