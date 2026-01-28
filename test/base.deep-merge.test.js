import test from 'node:test';
import assert from 'node:assert/strict';

import { deepMerge, isPlainObject, DefaultEventManagerConfig } from '../dist/base/index.js';

test('isPlainObject identifies plain objects and excludes arrays/functions/null', () => {
	assert.equal(isPlainObject({}), true);
	assert.equal(isPlainObject({ a: 1 }), true);
	assert.equal(isPlainObject([]), false);
	assert.equal(isPlainObject(null), false);
	assert.equal(isPlainObject(() => {}), false);
	assert.equal(isPlainObject(new Date()), false);
});

test('deepMerge merges nested objects and unions arrays by default', () => {
	const a = {
		left: 1,
		nested: { a: 1, arr: [1, 2] },
		list: [1, 2]
	};
	const b = {
		right: 2,
		nested: { b: 2, arr: [2, 3] },
		list: [2, 3]
	};

	const res = deepMerge(a, b);
	assert.deepEqual(res, {
		left: 1,
		right: 2,
		nested: { a: 1, b: 2, arr: [1, 2, 3] },
		list: [1, 2, 3]
	});
});

test('deepMerge array_strategy concat preserves duplicates', () => {
	const res = deepMerge(
		{ list: [1, 2] },
		{ list: [2, 3] },
		'concat'
	);
	assert.deepEqual(res, { list: [1, 2, 2, 3] });
});

test('deepMerge array_strategy replace replaces arrays from right side', () => {
	const res = deepMerge(
		{ list: [1, 2] },
		{ list: [2, 3] },
		'replace'
	);
	assert.deepEqual(res, { list: [2, 3] });
});

test('deepMerge returns right-hand value when left is not a plain object', () => {
	const res = deepMerge([1, 2], [3]);
	assert.deepEqual(res, [3]);
});

test('DefaultEventManagerConfig merges with overrides', () => {
	const res = deepMerge(DefaultEventManagerConfig, {
		merge_objects: false,
		array_strategy: 'concat'
	});
	assert.deepEqual(res, {
		merge_objects: false,
		array_strategy: 'concat',
		rollback_on_cancel: true
	});
});
