# @oglofus/drizzle-events

A lightweight, type-safe event layer for Drizzle ORM operations. It lets you hook into CRUD lifecycles (pre/post insert, update, delete) to validate, transform, and optionally cancel operations, with optional automatic rollback for post-event cancellations.

Currently focuses on Drizzle SQLite via `drizzle-orm/sqlite-core`.

- Events: pre-insert, post-insert, pre-update, post-update, pre-delete, post-delete
- Cancel or modify payloads in pre events
- Optionally rollback changes if a post event is cancelled
- Deep merge support when updating JSON-like objects


## Installation

```bash
npm install @oglofus/drizzle-events @oglofus/event-manager drizzle-orm
```

This library is ESM-only.


## Quick start (SQLite)

```ts
import { sqliteTable, integer, text } from 'drizzle-orm/sqlite-core';
import { Database } from 'drizzle-orm/sqlite-core'; // your BaseSQLiteDatabase type/instance
import { SQLiteEventManager } from '@oglofus/drizzle-events/sqlite';

// Define a Drizzle table
export const users = sqliteTable('users', {
  id: integer('id').primaryKey({ autoIncrement: true }),
  email: text('email').notNull().unique(),
  profile: text('profile') // JSON string in this simple example
});

// Create your Drizzle database instance "db" with sqlite driver of your choice
// const db: BaseSQLiteDatabase = ...

const events = new SQLiteEventManager(db, {
  merge_objects: true,          // Deep-merge objects on update
  array_strategy: 'union',      // Array merge strategy: 'replace' | 'concat' | 'union'
  rollback_on_cancel: true      // Rollback when a post-event is cancelled
});

// Add a pre-insert validation and transformation
events.put(users, 'pre-insert', (ev) => {
  const data = ev.data;

  if (!('email' in data) || typeof data.email !== 'string') {
    ev.cancel('email is required');
    return;
  }

  // Example transformation
  ev.data = {
    ...data,
    email: data.email.toLowerCase()
  };
});

// Insert with events
const result = await events.insert(users, 'id', { email: 'John@Example.com' });
if (result.type === 'error') {
  console.error('Insert failed:', result.message);
} else {
  console.log('Inserted row:', result.data);
}
```


## API

### SQLiteEventManager

```ts
new SQLiteEventManager(database, config)
```

- `database`: A Drizzle BaseSQLiteDatabase instance
- `config` (optional):
  - `merge_objects: boolean` — Deep-merge nested objects on update (default: `true`)
  - `array_strategy: 'replace' | 'concat' | 'union'` — How to merge arrays when `merge_objects` is enabled (default: `union`)
  - `rollback_on_cancel: boolean` — If a post-event is cancelled, revert the change (default: `true`)

Methods:

- `insert(table, primary_field, data)`
  - Emits `pre-insert` with a mutable `data` payload
  - On success, emits `post-insert` with the inserted `row`
  - Returns `{ type: 'success', data: row }` or `{ type: 'error', message }`
  - If `post-insert` is cancelled and `rollback_on_cancel` is true, the inserted row is deleted

- `update(table, primary_field, primary_value, data)`
  - Emits `pre-update` with a mutable `data` and the current `row`
  - If `merge_objects` is true, object fields in `data` are deep-merged into existing row values
  - On success, emits `post-update` with `row` and `old_row`
  - Returns `{ type: 'success', data: row }` or `{ type: 'error', message }`
  - If `post-update` is cancelled and `rollback_on_cancel` is true, reverts to `old_row`

- `delete(table, primary_field, primary_value)`
  - Emits `pre-delete` with the current `row`
  - On success, emits `post-delete` with the deleted `row`
  - Returns `{ type: 'success', data: row }` or `{ type: 'error', message }`
  - If `post-delete` is cancelled and `rollback_on_cancel` is true, reinserts the `row`


### Registering handlers

```ts
events.put(table, type, handler, priority)
```

- `type`: one of `'pre-insert' | 'post-insert' | 'pre-update' | 'post-update' | 'pre-delete' | 'post-delete'`
- `handler(event)`: function receiving a cancellable event object
- `priority` (optional): number (higher runs earlier). Uses priorities from `@oglofus/event-manager`.

Event objects share a common cancellable interface from `@oglofus/event-manager`:

- `event.cancel(reason?: string)` — marks the event as cancelled
- `event.isCancelled()` — returns boolean
- `event.getCancelReason()` — string | undefined

Per event type:

- `pre-insert` — `event.data`: insert payload (mutable)
- `post-insert` — `event.row`: inserted row (readonly)
- `pre-update` — `event.data` (mutable), `event.row` (current row before update)
- `post-update` — `event.row` (updated row), `event.old_row` (previous row)
- `pre-delete` — `event.row` (row to be deleted)
- `post-delete` — `event.row` (deleted row)


## Deep merge behavior on update

When `merge_objects` is enabled (default), and you pass partial objects in `data` to `update`, nested objects are deep-merged with existing row values. Arrays use the configured `array_strategy`:

- `replace` — replace existing array with the new one
- `concat` — concatenate arrays
- `union` — unique set union (default)


## Error handling

All public operations return a discriminated union:

```ts
type Response<T> =
  | { type: 'success'; data: T }
  | { type: 'error'; message?: string };
```

Check `result.type` to branch success vs error.


## Import paths

- Full package: `@oglofus/drizzle-events`
- SQLite module: `@oglofus/drizzle-events/sqlite`
- Base utilities/types: `@oglofus/drizzle-events/base`

Examples:

```ts
import { SQLiteEventManager } from '@oglofus/drizzle-events/sqlite';
import { deepMerge } from '@oglofus/drizzle-events/base';
```


## Notes

- This package builds on top of `@oglofus/event-manager` for the core event system.
- Current focus is SQLite; additional dialects can follow a similar pattern.


## License

ISC © oglofus
