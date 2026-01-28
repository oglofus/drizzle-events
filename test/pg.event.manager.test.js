import test from 'node:test';
import assert from 'node:assert/strict';

import { pgTable, integer, text, primaryKey } from 'drizzle-orm/pg-core';
import { getTableUniqueName } from 'drizzle-orm';
import { EventPriority } from '@oglofus/event-manager';

import { PgEventManager } from '../dist/pg/index.js';

const cloneValue = (value) => {
	if (typeof structuredClone === 'function') {
		return structuredClone(value);
	}

	return JSON.parse(JSON.stringify(value));
};

const extractConditions = (condition) => {
	const conditions = [];
	let pendingColumn = null;

	const isColumn = (value) =>
		value && typeof value === 'object' && 'columnType' in value && 'name' in value;
	const isParam = (value) => value && typeof value === 'object' && 'encoder' in value;

	const walk = (node) => {
		if (!node) return;
		if (Array.isArray(node)) {
			for (const item of node) walk(item);
			return;
		}
		if (typeof node !== 'object') return;

		if ('queryChunks' in node && Array.isArray(node.queryChunks)) {
			for (const chunk of node.queryChunks) walk(chunk);
			return;
		}

		if (isColumn(node)) {
			pendingColumn = node;
			return;
		}

		if (isParam(node) && pendingColumn) {
			conditions.push({ column: pendingColumn, value: node.value });
			pendingColumn = null;
		}
	};

	walk(condition);
	return conditions;
};

// Minimal in-memory mock for PgDatabase used by PgEventManager.
class MockPgDatabase {
	constructor() {
		this._store = new Map();
	}

	_getTableKey(table) {
		return getTableUniqueName(table);
	}

	_getRows(table) {
		const key = this._getTableKey(table);
		if (!this._store.has(key)) this._store.set(key, []);
		return this._store.get(key);
	}

	_snapshot() {
		const snapshot = new Map();
		for (const [key, rows] of this._store.entries()) {
			snapshot.set(
				key,
				rows.map((row) => cloneValue(row))
			);
		}
		return snapshot;
	}

	_restore(snapshot) {
		this._store = snapshot;
	}

	_matchRows(table, condition) {
		const rows = this._getRows(table);
		const conditions = extractConditions(condition);
		if (!conditions.length) return rows;
		return rows.filter((row) =>
			conditions.every(({ column, value }) => row[column.name] === value)
		);
	}

	select(/* fields */) {
		const self = this;
		return {
			from(table) {
				return {
					where(condition) {
						const rows = self._matchRows(table, condition).map((row) => cloneValue(row));
						return Promise.resolve(rows);
					}
				};
			}
		};
	}

	insert(table) {
		const self = this;
		return {
			values(data) {
				return {
					async returning() {
						const rows = self._getRows(table);
						const payload = Array.isArray(data) ? data : [data];
						const inserted = payload.map((row) => ({ ...row }));
						for (const row of inserted) rows.push(row);
						return inserted.map((row) => ({ ...row }));
					}
				};
			}
		};
	}

	update(table) {
		const self = this;
		return {
			set(data) {
				return {
					where(condition) {
						return {
							async returning() {
								const rows = self._matchRows(table, condition);
								if (!rows.length) return [];
								for (const row of rows) Object.assign(row, data);
								return rows.map((row) => ({ ...row }));
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
			async where(condition) {
				const rows = self._getRows(table);
				const toDelete = new Set(self._matchRows(table, condition));
				const remaining = rows.filter((row) => !toDelete.has(row));
				rows.splice(0, rows.length, ...remaining);
			}
		};
	}

	async transaction(fn) {
		const snapshot = this._snapshot();
		try {
			const res = await fn(this);
			return res;
		} catch (error) {
			this._restore(snapshot);
			throw error;
		}
	}
}

const users = pgTable('users', {
	id: integer('id').primaryKey(),
	name: text('name'),
	meta: text('meta')
});

const memberships = pgTable(
	'memberships',
	{
		user_id: integer('user_id').notNull(),
		org_id: integer('org_id').notNull(),
		role: text('role')
	},
	(table) => ({
		pk: primaryKey({ columns: [table.user_id, table.org_id] })
	})
);

const noPk = pgTable('no_pk', {
	name: text('name')
});

function createManager(db, config) {
	return new PgEventManager(db, config);
}

// Tests

test('pg insert: pre and post events run, data can be modified', async () => {
	const db = new MockPgDatabase();
	const manager = createManager(db);
	const calls = [];

	manager.put(
		users,
		'pre-insert',
		(event) => {
			calls.push('pre');
			event.data.name = 'Bob';
		},
		EventPriority.HIGH
	);

	manager.put(
		users,
		'post-insert',
		(event) => {
			calls.push('post');
			assert.equal(event.row.name, 'Bob');
		},
		EventPriority.NORMAL
	);

	const res = await manager.insert(users, { id: 1, name: 'Alice', meta: { a: 1 } });
	assert.equal(res.type, 'success');
	assert.equal(res.data.name, 'Bob');
	assert.deepEqual(calls, ['pre', 'post']);
});

test('pg insert: post-insert cancellation rolls back when enabled', async () => {
	const db = new MockPgDatabase();
	const manager = createManager(db, { rollback_on_cancel: true });

	manager.put(users, 'post-insert', (event) => event.cancel('rollback'));

	const res = await manager.insert(users, { id: 1, name: 'Alice' });
	assert.equal(res.type, 'error');
	assert.equal(res.message, 'rollback');

	const rows = await db.select({}).from(users).where();
	assert.equal(rows.length, 0);
});

test('pg insert: post-insert cancellation does not rollback when disabled', async () => {
	const db = new MockPgDatabase();
	const manager = createManager(db, { rollback_on_cancel: false });

	manager.put(users, 'post-insert', (event) => event.cancel('no-rollback'));

	const res = await manager.insert(users, { id: 1, name: 'Alice' });
	assert.equal(res.type, 'error');
	assert.equal(res.message, 'no-rollback');

	const rows = await db.select({}).from(users).where();
	assert.equal(rows.length, 1);
	assert.equal(rows[0].name, 'Alice');
});

test('pg update: merges objects and rolls back on post-update cancellation', async () => {
	const db = new MockPgDatabase();
	const manager = createManager(db, { merge_objects: true, rollback_on_cancel: true });

	await db
		.insert(users)
		.values({ id: 1, name: 'Alice', meta: { a: 1, arr: [1] } })
		.returning();

	manager.put(users, 'pre-update', (event) => {
		event.data.name = 'Alice2';
		event.data.meta = { b: 2, arr: [1, 2] };
	});

	const res = await manager.update(users, 'id', 1, {});
	assert.equal(res.type, 'success');
	assert.deepEqual(res.data, {
		id: 1,
		name: 'Alice2',
		meta: { a: 1, b: 2, arr: [1, 2] }
	});

	const manager2 = createManager(db, { merge_objects: true, rollback_on_cancel: true });
	manager2.put(users, 'post-update', (event) => event.cancel('undo'));

	const res2 = await manager2.update(users, 'id', 1, { name: 'Changed' });
	assert.equal(res2.type, 'error');
	assert.equal(res2.message, 'undo');

	const [rowAfterRollback] = await db.select({}).from(users).where();
	assert.deepEqual(rowAfterRollback, {
		id: 1,
		name: 'Alice2',
		meta: { a: 1, b: 2, arr: [1, 2] }
	});
});

test('pg delete: post-delete cancellation rolls back when enabled', async () => {
	const db = new MockPgDatabase();
	const manager = createManager(db, { rollback_on_cancel: true });

	await db.insert(users).values({ id: 1, name: 'Alice' }).returning();

	manager.put(users, 'post-delete', (event) => event.cancel('restore'));

	const res = await manager.delete(users, 'id', 1);
	assert.equal(res.type, 'error');
	assert.equal(res.message, 'restore');

	const [row] = await db.select({}).from(users).where();
	assert.equal(row.name, 'Alice');
});

test('pg delete: post-delete cancellation does not rollback when disabled', async () => {
	const db = new MockPgDatabase();
	const manager = createManager(db, { rollback_on_cancel: false });

	await db.insert(users).values({ id: 1, name: 'Alice' }).returning();

	manager.put(users, 'post-delete', (event) => event.cancel('gone'));

	const res = await manager.delete(users, 'id', 1);
	assert.equal(res.type, 'error');
	assert.equal(res.message, 'gone');

	const rows = await db.select({}).from(users).where();
	assert.equal(rows.length, 0);
});

test('pg update: composite key tables require explicit primary_field', async () => {
	const db = new MockPgDatabase();
	const manager = createManager(db);

	const res = await manager.update(memberships, { user_id: 1 }, { role: 'admin' });
	assert.equal(res.type, 'error');
	assert.equal(
		res.message,
		'Unable to resolve primary key columns for this table. Pass a primary_field explicitly.'
	);
});

test('pg update: missing primary key reports error', async () => {
	const db = new MockPgDatabase();
	const manager = createManager(db);

	const res = await manager.update(noPk, { name: 'Alice' }, { name: 'Bob' });
	assert.equal(res.type, 'error');
	assert.equal(
		res.message,
		'No primary key is defined for this table. Pass a primary_field explicitly.'
	);
});

test('pg update: returns error when row does not exist', async () => {
	const db = new MockPgDatabase();
	const manager = createManager(db);

	const res = await manager.update(users, 'id', 999, { name: 'Missing' });
	assert.equal(res.type, 'error');
	assert.equal(res.message, 'The row does not exist.');
});
