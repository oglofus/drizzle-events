import { EventPriority, RawEventManager } from '@oglofus/event-manager';
import {
	and,
	eq,
	getColumns,
	getTableUniqueName,
	InferInsertModel,
	InferSelectModel
} from 'drizzle-orm';
import {
	BaseSQLiteDatabase,
	getTableConfig,
	SQLiteTable,
	SQLiteUpdateSetSource
} from 'drizzle-orm/sqlite-core';
import {
	deepMerge,
	DefaultEventManagerConfig,
	errorResponse,
	EventManagerConfig,
	IssueEvent,
	PartialEventManagerConfig,
	Response,
	successResponse
} from '../base/index.js';

export class SQLitePreInsertEvent<T extends SQLiteTable> extends IssueEvent<InferSelectModel<T>> {
	private readonly _data: InferInsertModel<T>;

	constructor(
		fields: readonly Extract<keyof InferSelectModel<T>, string>[],
		data: InferInsertModel<T>
	) {
		super(fields);

		this._data = data;
	}

	get data(): InferInsertModel<T> {
		return this._data;
	}
}

export class SQLitePostInsertEvent<T extends SQLiteTable> extends IssueEvent<InferSelectModel<T>> {
	private readonly _row: InferSelectModel<T>;

	constructor(
		fields: readonly Extract<keyof InferSelectModel<T>, string>[],
		data: InferSelectModel<T>
	) {
		super(fields);

		this._row = data;
	}

	get row(): InferSelectModel<T> {
		return this._row;
	}
}

export class SQLitePreUpdateEvent<T extends SQLiteTable> extends IssueEvent<InferSelectModel<T>> {
	private readonly _data: SQLiteUpdateSetSource<T>;
	private readonly _row: InferSelectModel<T>;

	constructor(
		fields: readonly Extract<keyof InferSelectModel<T>, string>[],
		data: SQLiteUpdateSetSource<T>,
		row: InferSelectModel<T>
	) {
		super(fields);

		this._data = data;
		this._row = row;
	}

	get data(): SQLiteUpdateSetSource<T> {
		return this._data;
	}

	get row(): InferSelectModel<T> {
		return this._row;
	}
}

export class SQLitePostUpdateEvent<T extends SQLiteTable> extends IssueEvent<InferSelectModel<T>> {
	private readonly _row: InferSelectModel<T>;
	private readonly _old_row: InferSelectModel<T>;

	constructor(
		fields: readonly Extract<keyof InferSelectModel<T>, string>[],
		row: InferSelectModel<T>,
		old_row: InferSelectModel<T>
	) {
		super(fields);

		this._row = row;
		this._old_row = old_row;
	}

	get row(): InferSelectModel<T> {
		return this._row;
	}

	get old_row(): InferSelectModel<T> {
		return this._old_row;
	}
}

export class SQLitePreDeleteEvent<T extends SQLiteTable> extends IssueEvent<InferSelectModel<T>> {
	private readonly _row: InferSelectModel<T>;

	constructor(
		fields: readonly Extract<keyof InferSelectModel<T>, string>[],
		row: InferSelectModel<T>
	) {
		super(fields);

		this._row = row;
	}

	get row(): InferSelectModel<T> {
		return this._row;
	}
}

export class SQLitePostDeleteEvent<T extends SQLiteTable> extends IssueEvent<InferSelectModel<T>> {
	private readonly _row: InferSelectModel<T>;

	constructor(
		fields: readonly Extract<keyof InferSelectModel<T>, string>[],
		row: InferSelectModel<T>
	) {
		super(fields);

		this._row = row;
	}

	get row(): InferSelectModel<T> {
		return this._row;
	}
}

export type SQLiteEventType =
	| 'pre-insert'
	| 'post-insert'
	| 'pre-update'
	| 'post-update'
	| 'pre-delete'
	| 'post-delete';

export type SQLiteEventClass<
	T extends SQLiteTable,
	E extends SQLiteEventType
> = E extends 'pre-insert'
	? SQLitePreInsertEvent<T>
	: E extends 'post-insert'
		? SQLitePostInsertEvent<T>
		: E extends 'pre-update'
			? SQLitePreUpdateEvent<T>
			: E extends 'post-update'
				? SQLitePostUpdateEvent<T>
				: E extends 'pre-delete'
					? SQLitePreDeleteEvent<T>
					: SQLitePostDeleteEvent<T>;

export class SQLiteEventManager<
	D extends BaseSQLiteDatabase<any, any, any>
> extends RawEventManager {
	protected readonly _config: EventManagerConfig;
	protected readonly _database: D;

	constructor(database: D, config?: PartialEventManagerConfig) {
		super();

		this._database = database;
		this._config = deepMerge(DefaultEventManagerConfig, config ?? {});
	}

	get database(): D {
		return this._database;
	}

	get config(): EventManagerConfig {
		return this._config;
	}

	public async insert<T extends SQLiteTable>(
		table: T,
		data: InferInsertModel<T>
	): Promise<Response<InferSelectModel<T>>>;

	public async insert<T extends SQLiteTable>(
		table: T,
		primary_field: keyof InferSelectModel<T>,
		data: InferInsertModel<T>
	): Promise<Response<InferSelectModel<T>>>;

	public async insert<T extends SQLiteTable>(
		table: T,
		primary_field_or_data: keyof InferSelectModel<T> | InferInsertModel<T>,
		maybe_data?: InferInsertModel<T>
	): Promise<Response<InferSelectModel<T>>> {
		const issue_fields = this._getIssueFields(table);
		const issues = [];
		const primary_field =
			maybe_data === undefined ? undefined : (primary_field_or_data as keyof InferSelectModel<T>);

		let data =
			maybe_data === undefined ? (primary_field_or_data as InferInsertModel<T>) : maybe_data;

		const primary_info = this._resolvePrimaryKeys(table, primary_field);

		if ('error' in primary_info && this._config.rollback_on_cancel) {
			return errorResponse(
				`${primary_info.error} Pass a primary_field or disable rollback_on_cancel.`
			);
		}

		const pre_response = await this.run(
			table,
			'pre-insert',
			new SQLitePreInsertEvent<T>(issue_fields, data)
		);
		issues.push(...pre_response.event.issues);

		if (pre_response.event.isCancelled()) {
			return errorResponse(pre_response.event.getCancelReason(), [...issues]);
		}

		data = pre_response.event.data;

		let results: InferSelectModel<T>[];

		try {
			results = (await this._database
				.insert(table)
				.values(data)
				.returning()) as InferSelectModel<T>[];
		} catch (error) {
			return errorResponse('An error occurred while inserting the data.');
		}

		if (results.length === 0) {
			return errorResponse('An error occurred while inserting the data.');
		}

		const row = results[0];

		const post_response = await this.run(
			table,
			'post-insert',
			new SQLitePostInsertEvent<T>(issue_fields, row)
		);
		issues.push(...post_response.event.issues);

		if (post_response.event.isCancelled()) {
			if (this._config.rollback_on_cancel && 'keys' in primary_info) {
				await this._database
					.delete(table)
					.where(this._buildWhereFromKeys(table, primary_info.keys, row))
					.execute();
			}

			return errorResponse(post_response.event.getCancelReason(), [...issues]);
		}

		return successResponse(row, [...issues]);
	}

	public async update<T extends SQLiteTable>(
		table: T,
		primary_value: InferSelectModel<T>[keyof InferSelectModel<T>] | Partial<InferSelectModel<T>>,
		data: SQLiteUpdateSetSource<T>
	): Promise<Response<InferSelectModel<T>>>;

	public async update<T extends SQLiteTable>(
		table: T,
		primary_field: keyof InferSelectModel<T>,
		primary_value: InferSelectModel<T>[keyof InferSelectModel<T>],
		data: SQLiteUpdateSetSource<T>
	): Promise<Response<InferSelectModel<T>>>;

	public async update<T extends SQLiteTable>(
		table: T,
		primary_field_or_value:
			| keyof InferSelectModel<T>
			| InferSelectModel<T>[keyof InferSelectModel<T>]
			| Partial<InferSelectModel<T>>,
		primary_value_or_data:
			| InferSelectModel<T>[keyof InferSelectModel<T>]
			| SQLiteUpdateSetSource<T>,
		maybe_data?: SQLiteUpdateSetSource<T>
	): Promise<Response<InferSelectModel<T>>> {
		const issue_fields = this._getIssueFields(table);
		const issues = [];
		const primary_field =
			maybe_data === undefined ? undefined : (primary_field_or_value as keyof InferSelectModel<T>);
		const primary_value = maybe_data === undefined ? primary_field_or_value : primary_value_or_data;

		let data =
			maybe_data === undefined ? (primary_value_or_data as SQLiteUpdateSetSource<T>) : maybe_data;

		const primary_info = this._resolvePrimaryKeys(table, primary_field);

		if ('error' in primary_info) {
			return errorResponse(`${primary_info.error} Pass a primary_field explicitly.`);
		}

		const where_result = this._buildWhereFromPrimaryValue(table, primary_info.keys, primary_value);

		if ('error' in where_result) {
			return errorResponse(where_result.error);
		}

		const old_row = await this._database
			.select({
				...getColumns(table)
			})
			.from(table)
			.where(where_result.where)
			.get();

		if (!old_row) {
			return errorResponse('The row does not exist.');
		}

		const pre_response = await this.run(
			table,
			'pre-update',
			new SQLitePreUpdateEvent<T>(issue_fields, data, old_row)
		);
		issues.push(...pre_response.event.issues);

		if (pre_response.event.isCancelled()) {
			return errorResponse(pre_response.event.getCancelReason(), [...issues]);
		}

		data = pre_response.event.data;

		if (this._config.merge_objects) {
			for (const key in old_row) {
				if (!data[key]) {
					continue;
				}

				// @ts-ignore
				data[key] = deepMerge(old_row[key], data[key], this._config.array_strategy);
			}
		}

		let results: InferSelectModel<T>[];

		try {
			results = (await this._database
				.update(table)
				.set(data)
				.where(where_result.where)
				.returning()) as InferSelectModel<T>[];
		} catch (error) {
			return errorResponse('An error occurred while updating the data.');
		}

		if (results.length === 0) {
			return errorResponse('An error occurred while updating the data.');
		}

		const row = results[0];

		const post_response = await this.run(
			table,
			'post-update',
			new SQLitePostUpdateEvent<T>(issue_fields, row, old_row)
		);
		issues.push(...post_response.event.issues);

		if (post_response.event.isCancelled()) {
			if (this._config.rollback_on_cancel) {
				await this._database
					.update(table)
					.set(old_row)
					.where(this._buildWhereFromKeys(table, primary_info.keys, old_row))
					.execute();
			}

			return errorResponse(post_response.event.getCancelReason(), [...issues]);
		}

		return successResponse(row, [...issues]);
	}

	public async delete<T extends SQLiteTable>(
		table: T,
		primary_value: InferSelectModel<T>[keyof InferSelectModel<T>] | Partial<InferSelectModel<T>>
	): Promise<Response<InferSelectModel<T>>>;

	public async delete<T extends SQLiteTable>(
		table: T,
		primary_field: keyof InferSelectModel<T>,
		primary_value: InferSelectModel<T>[keyof InferSelectModel<T>]
	): Promise<Response<InferSelectModel<T>>>;

	public async delete<T extends SQLiteTable>(
		table: T,
		primary_field_or_value:
			| keyof InferSelectModel<T>
			| InferSelectModel<T>[keyof InferSelectModel<T>]
			| Partial<InferSelectModel<T>>,
		maybe_primary_value?: InferSelectModel<T>[keyof InferSelectModel<T>]
	): Promise<Response<InferSelectModel<T>>> {
		const issue_fields = this._getIssueFields(table);
		const issues = [];
		const primary_field =
			maybe_primary_value === undefined
				? undefined
				: (primary_field_or_value as keyof InferSelectModel<T>);
		const primary_value =
			maybe_primary_value === undefined ? primary_field_or_value : maybe_primary_value;

		const primary_info = this._resolvePrimaryKeys(table, primary_field);

		if ('error' in primary_info) {
			return errorResponse(`${primary_info.error} Pass a primary_field explicitly.`);
		}

		const where_result = this._buildWhereFromPrimaryValue(table, primary_info.keys, primary_value);

		if ('error' in where_result) {
			return errorResponse(where_result.error);
		}

		const row = await this._database
			.select({
				...getColumns(table)
			})
			.from(table)
			.where(where_result.where)
			.get();

		if (!row) {
			return errorResponse('The row does not exist.');
		}

		const pre_response = await this.run(
			table,
			'pre-delete',
			new SQLitePreDeleteEvent<T>(issue_fields, row)
		);
		issues.push(...pre_response.event.issues);

		if (pre_response.event.isCancelled()) {
			return errorResponse(pre_response.event.getCancelReason(), [...issues]);
		}

		try {
			await this._database.delete(table).where(where_result.where);
		} catch (error) {
			return errorResponse('An error occurred while deleting the data.');
		}

		const post_response = await this.run(
			table,
			'post-delete',
			new SQLitePostDeleteEvent<T>(issue_fields, row)
		);
		issues.push(...post_response.event.issues);

		if (post_response.event.isCancelled()) {
			if (this._config.rollback_on_cancel) {
				await this._database.insert(table).values(row).execute();
			}

			return errorResponse(post_response.event.getCancelReason(), [...issues]);
		}

		return successResponse(row, [...issues]);
	}

	public put<T extends SQLiteTable, E extends SQLiteEventType, C extends SQLiteEventClass<T, E>>(
		table: T,
		type: E,
		handler: (event: C) => Promise<void> | void,
		priority: EventPriority = EventPriority.NORMAL
	) {
		return this._register(this.getEventKey(table, type), handler, priority);
	}

	public async run<
		T extends SQLiteTable,
		E extends SQLiteEventType,
		C extends SQLiteEventClass<T, E>
	>(table: T, type: E, event: C) {
		return await this._emit(this.getEventKey(table, type), event);
	}

	protected _resolvePrimaryKeys<T extends SQLiteTable>(
		table: T,
		primary_field?: keyof InferSelectModel<T>
	): { keys: (keyof InferSelectModel<T>)[] } | { error: string } {
		if (primary_field) {
			return { keys: [primary_field] };
		}

		const config = getTableConfig(table);
		const primary_columns = config.primaryKeys.flatMap((pk) => pk.columns);

		if (primary_columns.length === 0) {
			return { error: 'No primary key is defined for this table.' };
		}

		const table_columns = getColumns(table);
		const keys: (keyof InferSelectModel<T>)[] = [];

		for (const primary_column of primary_columns) {
			const entry = Object.entries(table_columns).find(([, column]) => column === primary_column);

			if (!entry) {
				continue;
			}

			const key = entry[0] as keyof InferSelectModel<T>;

			if (!keys.includes(key)) {
				keys.push(key);
			}
		}

		if (keys.length === 0) {
			return { error: 'Unable to resolve primary key columns for this table.' };
		}

		return { keys };
	}

	protected _buildWhereFromPrimaryValue<T extends SQLiteTable>(
		table: T,
		keys: (keyof InferSelectModel<T>)[],
		primary_value: unknown
	): { where: ReturnType<typeof eq> } | { error: string } {
		const table_columns = getColumns(table);

		if (keys.length === 1) {
			const key = keys[0];
			const value =
				primary_value !== null &&
				typeof primary_value === 'object' &&
				key in (primary_value as Record<string, unknown>)
					? (primary_value as Record<string, unknown>)[key]
					: primary_value;

			return { where: eq(table_columns[key], value as any) };
		}

		if (primary_value === null || typeof primary_value !== 'object') {
			return {
				error: `Composite primary key requires an object with values for ${keys
					.map((key) => `"${String(key)}"`)
					.join(', ')}.`
			};
		}

		for (const key of keys) {
			if (!(key in (primary_value as Record<string, unknown>))) {
				return { error: `Composite primary key requires a value for "${String(key)}".` };
			}
		}

		return {
			where: this._buildWhereFromKeys(table, keys, primary_value as Partial<InferSelectModel<T>>)
		};
	}

	protected _buildWhereFromKeys<T extends SQLiteTable>(
		table: T,
		keys: (keyof InferSelectModel<T>)[],
		values: Partial<InferSelectModel<T>>
	) {
		const table_columns = getColumns(table);
		const clauses = keys.map((key) => eq(table_columns[key], values[key] as any));
		return clauses.length === 1 ? clauses[0] : and(...clauses);
	}

	protected _getIssueFields<T extends SQLiteTable>(table: T) {
		return Object.keys(getColumns(table)) as Extract<keyof InferSelectModel<T>, string>[];
	}

	private getEventKey(table: SQLiteTable, type: SQLiteEventType) {
		return getTableUniqueName(table) + ':' + type;
	}
}
