# @oglofus/drizzle-events [![NPM Version](https://img.shields.io/npm/v/%40oglofus%2Fdrizzle-events)](https://www.npmjs.com/package/@oglofus/drizzle-events) [![Publish Package to NPM](https://github.com/oglofus/drizzle-events/actions/workflows/release-package.yml/badge.svg)](https://github.com/oglofus/drizzle-events/actions/workflows/release-package.yml)

A lightweight, strongly typed event layer for [Drizzle ORM](https://orm.drizzle.team/). Register handlers around insert, update, and delete operations to validate data, transform insert and update payloads, collect field-level issues, or cancel an operation.

The package currently provides managers for:

- SQLite databases
- Cloudflare D1 databases
- PostgreSQL databases

## Installation

```bash
# npm
npm install @oglofus/drizzle-events drizzle-orm

# pnpm
pnpm add @oglofus/drizzle-events drizzle-orm

# Yarn
yarn add @oglofus/drizzle-events drizzle-orm

# Bun
bun add @oglofus/drizzle-events drizzle-orm
```

The package is ESM-only. It requires Drizzle ORM; `@oglofus/event-manager` is installed as a package dependency.

## Import paths

```ts
import { type Response, deepMerge } from "@oglofus/drizzle-events/base";
import { D1EventManager } from "@oglofus/drizzle-events/d1";
import { PgEventManager } from "@oglofus/drizzle-events/pg";
import { SQLiteEventManager } from "@oglofus/drizzle-events/sqlite";
```

The package subpaths are:

- `@oglofus/drizzle-events/sqlite` — `SQLiteEventManager` and SQLite event classes/types
- `@oglofus/drizzle-events/d1` — `D1EventManager`, extending the SQLite manager with D1 batch inserts
- `@oglofus/drizzle-events/pg` — `PgEventManager` and PostgreSQL event classes/types
- `@oglofus/drizzle-events/base` — response, issue, configuration, and deep-merge utilities

## Quick start

The manager is constructed with a Drizzle database instance. The database must support the async operations used by the selected manager.

```ts
import { SQLiteEventManager } from "@oglofus/drizzle-events/sqlite";
import { integer, sqliteTable, text } from "drizzle-orm/sqlite-core";

const users = sqliteTable("users", {
  id: integer("id").primaryKey({ autoIncrement: true }),
  email: text("email").notNull().unique(),
});

// Create `db` with the SQLite driver of your choice.
// const db = ...;
const events = new SQLiteEventManager(db, {
  rollback_on_cancel: true,
});

events.put(users, "pre-insert", (event) => {
  if (!event.data.email) {
    event.cancel(event.issue.email("Email is required."));
    return;
  }

  // The payload object is mutable. Mutate its properties rather than
  // assigning a new value to event.data.
  event.data.email = event.data.email.toLowerCase();
});

events.put(users, "post-insert", (event) => {
  if (event.row.email.endsWith("@blocked.example")) {
    event.cancel("Blocked email domain.");
  }
});

const result = await events.insert(users, { email: "John@Example.com" });

if (result.type === "success") {
  console.log(result.data); // inserted row
  console.log(result.issues); // collected non-fatal issues
} else {
  console.error(result.message, result.issues);
}
```

## Event lifecycle

All managers support the following event types:

- `pre-insert` — runs before insertion; `event.data` is mutable
- `post-insert` — runs after insertion; `event.row` is the inserted row
- `pre-update` — runs before update; `event.data` is mutable and `event.row` is the existing row
- `post-update` — runs after update; `event.row` is the updated row and `event.old_row` is the previous row
- `pre-delete` — runs before deletion; `event.row` is the row to be deleted
- `post-delete` — runs after deletion; `event.row` is the deleted row

Register handlers with:

```ts
events.put(table, type, handler, priority);
```

Handlers may be synchronous or asynchronous. `priority` is optional and uses `EventPriority` from `@oglofus/event-manager`; higher-priority handlers run first.

Every event provides the cancellable event-manager API:

- `event.cancel(reason?)` — cancel the operation with an optional message
- `event.cancel(issue)` — cancel and add a structured issue
- `event.cancel(reason, issue1, issue2, ...)` — cancel with a message and issues
- `event.isCancelled()` — check whether the event was cancelled
- `event.getCancelReason()` — read the cancellation reason
- `event.issue` — create typed issues for table fields
- `event.addIssues(...issues)` — add non-cancelling issues
- `event.issues` — read the issues collected by the event

Issue helpers are generated from the table fields:

```ts
events.put(users, "pre-insert", (event) => {
  event.addIssues(event.issue.email("Email was normalized."));
  event.addIssues(
    event.issue.$root("A table-level warning."),
    event.issue.$path(["profile", "name"], "Name is invalid."),
  );
});
```

An `Issue` has a `message` and an optional `path`. Field helpers such as `event.issue.email(...)` create a path containing that field; `$root` creates an issue without a path.

## Database managers

### SQLite

```ts
new SQLiteEventManager(database, config?);
```

Methods:

```ts
await events.insert(table, data);
await events.insert(table, primary_field, data);

await events.update(table, primary_value, data);
await events.update(table, primary_field, primary_value, data);

await events.delete(table, primary_value);
await events.delete(table, primary_field, primary_value);
```

When the primary field is omitted, the manager resolves the table's primary key, including composite primary keys. For composite keys, pass an object containing the key values, for example `{ user_id: 1, org_id: 2 }`. An explicit `primary_field` can be used for tables with a single key or when the inferred key is not appropriate.

### Cloudflare D1

```ts
new D1EventManager(database, config?);
```

`D1EventManager` supports all SQLite manager methods and adds batch insertion through D1's `batch` API:

```ts
await events.insertBatch(table, data);
await events.insertBatch(table, primary_field, data);
```

Pre-insert handlers run for each row before the batch is sent. Post-insert handlers run for each returned row. With `rollback_on_cancel` enabled, a cancelled post-insert event removes all rows inserted by the batch.

### PostgreSQL

```ts
new PgEventManager(database, config?);
```

The single-row methods have the same signatures as the SQLite manager. PostgreSQL additionally supports batch operations:

```ts
await events.insert_batch(table, data);
await events.insert_batch(table, primary_field, data);

await events.update_batch(table, updates);
await events.update_batch(table, primary_field, updates);

await events.delete_batch(table, primary_values);
await events.delete_batch(table, primary_field, primary_values);
```

Each update in `update_batch` has this shape:

```ts
{
  primary_value: 1,
  data: { name: "Updated name" },
}
```

With `rollback_on_cancel` enabled, PostgreSQL operations that reach a cancelled post-event run inside a transaction and are rolled back. This also applies to batch operations. Disable the option if rollback is not desired.

## Configuration

The optional configuration is shared by all managers:

```ts
{
  merge_objects: true,
  array_strategy: "union",
  rollback_on_cancel: true,
}
```

Defaults:

- `merge_objects: true` — deep-merge plain object values during updates
- `array_strategy: "union"` — merge arrays using one of `"replace"`, `"concat"`, or `"union"`
- `rollback_on_cancel: true` — undo a database change when a post-event is cancelled

SQLite and D1 emulate rollback by issuing a compensating operation: deleting an inserted row, restoring an updated row, or reinserting a deleted row. PostgreSQL uses a transaction when rollback is enabled. Rollback requires resolvable primary-key information for SQLite and D1 inserts.

## Deep merge behavior

`deepMerge` is also available from the `base` subpath:

```ts
import { deepMerge } from "@oglofus/drizzle-events/base";

const merged = deepMerge(
  { profile: { name: "Ada", tags: ["admin"] } },
  { profile: { tags: ["editor"] } },
  "union",
);
```

Plain objects are merged recursively. Arrays use the selected strategy:

- `replace` — use the new array
- `concat` — append the new array, preserving duplicates
- `union` — combine values and remove duplicates

## Responses and errors

All public operations return a discriminated `Response<T>`:

```ts
import type { Response } from "@oglofus/drizzle-events/base";

type Response<T> =
  | {
      type: "success";
      data: T;
      issues: Issue[];
    }
  | {
      type: "error";
      message?: string;
      issues: Issue[];
    };
```

Always branch on `result.type`. `issues` is returned on both successful and failed operations, so handlers can report warnings or validation details along with the result.

## Development

The repository uses pnpm and requires Node.js with Corepack support enabled.

```bash
corepack enable
pnpm install
pnpm run build
pnpm test
pnpm run lint
```

Tests use Node's built-in test runner and cover the base utilities, SQLite, D1, and PostgreSQL managers.

## License

ISC © oglofus
