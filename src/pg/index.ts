import { CancellableEvent, EventPriority, RawEventManager } from '@oglofus/event-manager';
import {
	and,
	eq,
	getTableColumns,
	getTableUniqueName,
	InferInsertModel,
	InferSelectModel
} from 'drizzle-orm';
import {
	getTableConfig,
	PgDatabase,
	PgTableWithColumns,
	PgUpdateSetSource,
	TableConfig
} from 'drizzle-orm/pg-core';
import {
	deepMerge,
	DefaultEventManagerConfig,
	EventManagerConfig,
	PartialEventManagerConfig,
	Response
} from '../base/index.js';

class PgEventRollbackError extends Error {
	constructor(message?: string) {
		super(message);
		this.name = 'PgEventRollbackError';
	}
}

export class PgPreInsertEvent<T extends PgTableWithColumns<any>> extends CancellableEvent {
	private readonly _data: InferInsertModel<T>;

	constructor(data: InferInsertModel<T>) {
		super();

		this._data = data;
	}

	get data(): InferInsertModel<T> {
		return this._data;
	}
}

export class PgPostInsertEvent<T extends PgTableWithColumns<any>> extends CancellableEvent {
	private readonly _row: InferSelectModel<T>;

	constructor(data: InferSelectModel<T>) {
		super();

		this._row = data;
	}

	get row(): InferSelectModel<T> {
		return this._row;
	}
}

export class PgPreUpdateEvent<T extends PgTableWithColumns<any>> extends CancellableEvent {
	private readonly _data: PgUpdateSetSource<T>;
	private readonly _row: InferSelectModel<T>;

	constructor(data: PgUpdateSetSource<T>, row: InferSelectModel<T>) {
		super();

		this._data = data;
		this._row = row;
	}

	get data(): PgUpdateSetSource<T> {
		return this._data;
	}

	get row(): InferSelectModel<T> {
		return this._row;
	}
}

export class PgPostUpdateEvent<T extends PgTableWithColumns<any>> extends CancellableEvent {
	private readonly _row: InferSelectModel<T>;
	private readonly _old_row: InferSelectModel<T>;

	constructor(row: InferSelectModel<T>, old_row: InferSelectModel<T>) {
		super();

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

export class PgPreDeleteEvent<T extends PgTableWithColumns<any>> extends CancellableEvent {
	private readonly _row: InferSelectModel<T>;

	constructor(row: InferSelectModel<T>) {
		super();

		this._row = row;
	}

	get row(): InferSelectModel<T> {
		return this._row;
	}
}

export class PgPostDeleteEvent<T extends PgTableWithColumns<any>> extends CancellableEvent {
	private readonly _row: InferSelectModel<T>;

	constructor(row: InferSelectModel<T>) {
		super();

		this._row = row;
	}

	get row(): InferSelectModel<T> {
		return this._row;
	}
}

export type PgEventType =
	| 'pre-insert'
	| 'post-insert'
	| 'pre-update'
	| 'post-update'
	| 'pre-delete'
	| 'post-delete';

export type PgEventClass<
	T extends PgTableWithColumns<any>,
	E extends PgEventType
> = E extends 'pre-insert'
	? PgPreInsertEvent<T>
	: E extends 'post-insert'
		? PgPostInsertEvent<T>
		: E extends 'pre-update'
			? PgPreUpdateEvent<T>
			: E extends 'post-update'
				? PgPostUpdateEvent<T>
				: E extends 'pre-delete'
					? PgPreDeleteEvent<T>
					: PgPostDeleteEvent<T>;

export class PgEventManager<D extends PgDatabase<any, any, any>> extends RawEventManager {
	protected readonly _config: EventManagerConfig;
	protected readonly _database: D;

	constructor(database: D, config?: PartialEventManagerConfig) {
		super();

		this._database = database;
		this._config = deepMerge<EventManagerConfig, PartialEventManagerConfig>(
			DefaultEventManagerConfig,
			config ?? {}
		);
	}

	get database(): D {
		return this._database;
	}

	get config(): EventManagerConfig {
		return this._config;
	}

	public async insert<T extends PgTableWithColumns<any>>(
		table: T,
		data: InferInsertModel<T>
	): Promise<Response<InferSelectModel<T>>>;

	public async insert<T extends PgTableWithColumns<any>>(
		table: T,
		primary_field: keyof InferSelectModel<T>,
		data: InferInsertModel<T>
	): Promise<Response<InferSelectModel<T>>>;

	public async insert<T extends PgTableWithColumns<any>>(
		table: T,
		primary_field_or_data: keyof InferSelectModel<T> | InferInsertModel<T>,
		maybe_data?: InferInsertModel<T>
	): Promise<Response<InferSelectModel<T>>> {
		let data =
			maybe_data === undefined ? (primary_field_or_data as InferInsertModel<T>) : maybe_data;

		const runInsert = async (
			database: PgDatabase<any, any, any>
		): Promise<Response<InferSelectModel<T>>> => {
			const pre_response = await this.run(table, 'pre-insert', new PgPreInsertEvent<T>(data));

			if (pre_response.event.isCancelled()) {
				return {
					type: 'error',
					message: pre_response.event.getCancelReason()
				};
			}

			data = pre_response.event.data;
			let results: InferSelectModel<T>[];

			try {
				results = (await database.insert(table).values(data).returning()) as InferSelectModel<T>[];
			} catch (error) {
				return {
					type: 'error',
					message: 'An error occurred while inserting the data.'
				};
			}

			if (results.length === 0) {
				return {
					type: 'error',
					message: 'An error occurred while inserting the data.'
				};
			}

			const row = results[0];

			const post_response = await this.run(table, 'post-insert', new PgPostInsertEvent<T>(row));

			if (post_response.event.isCancelled()) {
				if (this._config.rollback_on_cancel) {
					throw new PgEventRollbackError(post_response.event.getCancelReason());
				}

				return {
					type: 'error',
					message: post_response.event.getCancelReason()
				};
			}

			return {
				type: 'success',
				data: row
			};
		};

		if (this._config.rollback_on_cancel) {
			try {
				return await this._database.transaction(async (tx) => runInsert(tx));
			} catch (error) {
				if (error instanceof PgEventRollbackError) {
					return {
						type: 'error',
						message: error.message
					};
				}

				return {
					type: 'error',
					message: 'An error occurred while inserting the data.'
				};
			}
		}

		return await runInsert(this._database);
	}

	public async insert_batch<T extends PgTableWithColumns<any>>(
		table: T,
		data: InferInsertModel<T>[]
	): Promise<Response<InferSelectModel<T>[]>>;

	public async insert_batch<T extends PgTableWithColumns<any>>(
		table: T,
		primary_field: keyof InferSelectModel<T>,
		data: InferInsertModel<T>[]
	): Promise<Response<InferSelectModel<T>[]>>;

	public async insert_batch<T extends PgTableWithColumns<any>>(
		table: T,
		primary_field_or_data: keyof InferSelectModel<T> | InferInsertModel<T>[],
		maybe_data?: InferInsertModel<T>[]
	): Promise<Response<InferSelectModel<T>[]>> {
		let data =
			maybe_data === undefined ? (primary_field_or_data as InferInsertModel<T>[]) : maybe_data;

		const runInsertBatch = async (
			database: PgDatabase<any, any, any>,
			rollbackOnError: boolean
		): Promise<Response<InferSelectModel<T>[]>> => {
			for (let i = 0; i < data.length; i++) {
				const pre_response = await this.run(table, 'pre-insert', new PgPreInsertEvent<T>(data[i]));

				if (pre_response.event.isCancelled()) {
					const message = pre_response.event.getCancelReason();
					if (rollbackOnError) {
						throw new PgEventRollbackError(message);
					}

					return {
						type: 'error',
						message
					};
				}

				data[i] = pre_response.event.data;
			}

			let results: InferSelectModel<T>[];

			try {
				results = (await database.insert(table).values(data).returning()) as InferSelectModel<T>[];
			} catch (error) {
				const message = 'An error occurred while inserting the data.';
				if (rollbackOnError) {
					throw new PgEventRollbackError(message);
				}

				return {
					type: 'error',
					message
				};
			}

			if (results.length === 0) {
				const message = 'An error occurred while inserting the data.';
				if (rollbackOnError) {
					throw new PgEventRollbackError(message);
				}

				return {
					type: 'error',
					message
				};
			}

			for (let i = 0; i < results.length; i++) {
				const post_response = await this.run(
					table,
					'post-insert',
					new PgPostInsertEvent<T>(results[i])
				);

				if (post_response.event.isCancelled()) {
					const message = post_response.event.getCancelReason();
					if (rollbackOnError) {
						throw new PgEventRollbackError(message);
					}

					return {
						type: 'error',
						message
					};
				}
			}

			return {
				type: 'success',
				data: results
			};
		};

		if (this._config.rollback_on_cancel) {
			try {
				return await this._database.transaction(async (tx) => runInsertBatch(tx, true));
			} catch (error) {
				if (error instanceof PgEventRollbackError) {
					return {
						type: 'error',
						message: error.message
					};
				}

				return {
					type: 'error',
					message: 'An error occurred while inserting the data.'
				};
			}
		}

		return await runInsertBatch(this._database, false);
	}

	public async update<T extends PgTableWithColumns<any>>(
		table: T,
		primary_value: InferSelectModel<T>[keyof InferSelectModel<T>] | Partial<InferSelectModel<T>>,
		data: PgUpdateSetSource<T>
	): Promise<Response<InferSelectModel<T>>>;

	public async update<T extends PgTableWithColumns<any>>(
		table: T,
		primary_field: keyof InferSelectModel<T>,
		primary_value: InferSelectModel<T>[keyof InferSelectModel<T>],
		data: PgUpdateSetSource<T>
	): Promise<Response<InferSelectModel<T>>>;

	public async update<T extends PgTableWithColumns<any>>(
		table: T,
		primary_field_or_value:
			| keyof InferSelectModel<T>
			| InferSelectModel<T>[keyof InferSelectModel<T>]
			| Partial<InferSelectModel<T>>,
		primary_value_or_data: InferSelectModel<T>[keyof InferSelectModel<T>] | PgUpdateSetSource<T>,
		maybe_data?: PgUpdateSetSource<T>
	): Promise<Response<InferSelectModel<T>>> {
		const primary_field =
			maybe_data === undefined ? undefined : (primary_field_or_value as keyof InferSelectModel<T>);
		const primary_value = maybe_data === undefined ? primary_field_or_value : primary_value_or_data;

		let data =
			maybe_data === undefined ? (primary_value_or_data as PgUpdateSetSource<T>) : maybe_data;

		const primary_info = this._resolvePrimaryKeys(table, primary_field);

		if ('error' in primary_info) {
			return {
				type: 'error',
				message: `${primary_info.error} Pass a primary_field explicitly.`
			};
		}

		const where_result = this._buildWhereFromPrimaryValue(table, primary_info.keys, primary_value);

		if ('error' in where_result) {
			return {
				type: 'error',
				message: where_result.error
			};
		}

		const runUpdate = async (
			database: PgDatabase<any, any, any>
		): Promise<Response<InferSelectModel<T>>> => {
			const [old_row] = (await database
				.select({
					...getTableColumns(table)
				})
				.from(table as PgTableWithColumns<any>)
				.where(where_result.where)) as InferSelectModel<T>[];

			if (!old_row) {
				return {
					type: 'error',
					message: 'The row does not exist.'
				};
			}

			const pre_response = await this.run(
				table,
				'pre-update',
				new PgPreUpdateEvent<T>(data, old_row)
			);

			if (pre_response.event.isCancelled()) {
				return {
					type: 'error',
					message: pre_response.event.getCancelReason()
				};
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
				results = (await database
					.update(table)
					.set(data)
					.where(where_result.where)
					.returning()) as InferSelectModel<T>[];
			} catch (error) {
				return {
					type: 'error',
					message: 'An error occurred while updating the data.'
				};
			}

			if (results.length === 0) {
				return {
					type: 'error',
					message: 'An error occurred while updating the data.'
				};
			}

			const row = results[0];

			const post_response = await this.run(
				table,
				'post-update',
				new PgPostUpdateEvent<T>(row, old_row)
			);

			if (post_response.event.isCancelled()) {
				if (this._config.rollback_on_cancel) {
					throw new PgEventRollbackError(post_response.event.getCancelReason());
				}

				return {
					type: 'error',
					message: post_response.event.getCancelReason()
				};
			}

			return {
				type: 'success',
				data: row
			};
		};

		if (this._config.rollback_on_cancel) {
			try {
				return await this._database.transaction(async (tx) => runUpdate(tx));
			} catch (error) {
				if (error instanceof PgEventRollbackError) {
					return {
						type: 'error',
						message: error.message
					};
				}

				return {
					type: 'error',
					message: 'An error occurred while updating the data.'
				};
			}
		}

		return await runUpdate(this._database);
	}

	public async update_batch<T extends PgTableWithColumns<any>>(
		table: T,
		updates: Array<{
			primary_value: InferSelectModel<T>[keyof InferSelectModel<T>] | Partial<InferSelectModel<T>>;
			data: PgUpdateSetSource<T>;
		}>
	): Promise<Response<InferSelectModel<T>[]>>;

	public async update_batch<T extends PgTableWithColumns<any>>(
		table: T,
		primary_field: keyof InferSelectModel<T>,
		updates: Array<{
			primary_value: InferSelectModel<T>[keyof InferSelectModel<T>];
			data: PgUpdateSetSource<T>;
		}>
	): Promise<Response<InferSelectModel<T>[]>>;

	public async update_batch<T extends PgTableWithColumns<any>>(
		table: T,
		primary_field_or_updates:
			| keyof InferSelectModel<T>
			| Array<{
					primary_value:
						| InferSelectModel<T>[keyof InferSelectModel<T>]
						| Partial<InferSelectModel<T>>;
					data: PgUpdateSetSource<T>;
			  }>,
		maybe_updates?: Array<{
			primary_value: InferSelectModel<T>[keyof InferSelectModel<T>];
			data: PgUpdateSetSource<T>;
		}>
	): Promise<Response<InferSelectModel<T>[]>> {
		const primary_field =
			maybe_updates === undefined
				? undefined
				: (primary_field_or_updates as keyof InferSelectModel<T>);
		const updates =
			maybe_updates === undefined
				? (primary_field_or_updates as Array<{
						primary_value:
							| InferSelectModel<T>[keyof InferSelectModel<T>]
							| Partial<InferSelectModel<T>>;
						data: PgUpdateSetSource<T>;
					}>)
				: maybe_updates;

		const primary_info = this._resolvePrimaryKeys(table, primary_field);
		if ('error' in primary_info) {
			return {
				type: 'error',
				message: `${primary_info.error} Pass a primary_field explicitly.`
			};
		}

		const runUpdateBatch = async (
			database: PgDatabase<any, any, any>,
			rollbackOnError: boolean
		): Promise<Response<InferSelectModel<T>[]>> => {
			const rows: InferSelectModel<T>[] = [];

			for (const update of updates) {
				const where_result = this._buildWhereFromPrimaryValue(
					table,
					primary_info.keys,
					update.primary_value
				);

				if ('error' in where_result) {
					if (rollbackOnError) {
						throw new PgEventRollbackError(where_result.error);
					}

					return {
						type: 'error',
						message: where_result.error
					};
				}

				const [old_row] = (await database
					.select({
						...getTableColumns(table)
					})
					.from(table as PgTableWithColumns<any>)
					.where(where_result.where)) as InferSelectModel<T>[];

				if (!old_row) {
					const message = 'The row does not exist.';
					if (rollbackOnError) {
						throw new PgEventRollbackError(message);
					}

					return {
						type: 'error',
						message
					};
				}

				let data = update.data;

				const pre_response = await this.run(
					table,
					'pre-update',
					new PgPreUpdateEvent<T>(data, old_row)
				);

				if (pre_response.event.isCancelled()) {
					const message = pre_response.event.getCancelReason();
					if (rollbackOnError) {
						throw new PgEventRollbackError(message);
					}

					return {
						type: 'error',
						message
					};
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
					results = (await database
						.update(table)
						.set(data)
						.where(where_result.where)
						.returning()) as InferSelectModel<T>[];
				} catch (error) {
					const message = 'An error occurred while updating the data.';
					if (rollbackOnError) {
						throw new PgEventRollbackError(message);
					}

					return {
						type: 'error',
						message
					};
				}

				if (results.length === 0) {
					const message = 'An error occurred while updating the data.';
					if (rollbackOnError) {
						throw new PgEventRollbackError(message);
					}

					return {
						type: 'error',
						message
					};
				}

				const row = results[0];

				const post_response = await this.run(
					table,
					'post-update',
					new PgPostUpdateEvent<T>(row, old_row)
				);

				if (post_response.event.isCancelled()) {
					const message = post_response.event.getCancelReason();
					if (rollbackOnError) {
						throw new PgEventRollbackError(message);
					}

					return {
						type: 'error',
						message
					};
				}

				rows.push(row);
			}

			return {
				type: 'success',
				data: rows
			};
		};

		if (this._config.rollback_on_cancel) {
			try {
				return await this._database.transaction(async (tx) => runUpdateBatch(tx, true));
			} catch (error) {
				if (error instanceof PgEventRollbackError) {
					return {
						type: 'error',
						message: error.message
					};
				}

				return {
					type: 'error',
					message: 'An error occurred while updating the data.'
				};
			}
		}

		return await runUpdateBatch(this._database, false);
	}

	public async delete<T extends PgTableWithColumns<any>>(
		table: T,
		primary_value: InferSelectModel<T>[keyof InferSelectModel<T>] | Partial<InferSelectModel<T>>
	): Promise<Response<InferSelectModel<T>>>;

	public async delete<TB extends TableConfig, T extends PgTableWithColumns<TB>>(
		table: T,
		primary_field: keyof InferSelectModel<T>,
		primary_value: InferSelectModel<T>[keyof InferSelectModel<T>]
	): Promise<Response<InferSelectModel<T>>>;

	public async delete<T extends PgTableWithColumns<any>>(
		table: T,
		primary_field_or_value:
			| keyof InferSelectModel<T>
			| InferSelectModel<T>[keyof InferSelectModel<T>]
			| Partial<InferSelectModel<T>>,
		maybe_primary_value?: InferSelectModel<T>[keyof InferSelectModel<T>]
	): Promise<Response<InferSelectModel<T>>> {
		const primary_field =
			maybe_primary_value === undefined
				? undefined
				: (primary_field_or_value as keyof InferSelectModel<T>);
		const primary_value =
			maybe_primary_value === undefined ? primary_field_or_value : maybe_primary_value;
		const primary_info = this._resolvePrimaryKeys(table, primary_field);

		if ('error' in primary_info) {
			return {
				type: 'error',
				message: `${primary_info.error} Pass a primary_field explicitly.`
			};
		}

		const where_result = this._buildWhereFromPrimaryValue(table, primary_info.keys, primary_value);

		if ('error' in where_result) {
			return {
				type: 'error',
				message: where_result.error
			};
		}

		const runDelete = async (
			database: PgDatabase<any, any, any>
		): Promise<Response<InferSelectModel<T>>> => {
			const [row] = (await database
				.select({
					...getTableColumns(table)
				})
				.from(table as PgTableWithColumns<any>)
				.where(where_result.where)) as InferSelectModel<T>[];

			if (!row) {
				return {
					type: 'error',
					message: 'The row does not exist.'
				};
			}

			const pre_response = await this.run(table, 'pre-delete', new PgPreDeleteEvent<T>(row));

			if (pre_response.event.isCancelled()) {
				return {
					type: 'error',
					message: pre_response.event.getCancelReason()
				};
			}

			try {
				await database.delete(table).where(where_result.where);
			} catch (error) {
				return {
					type: 'error',
					message: 'An error occurred while updating the data.'
				};
			}

			const post_response = await this.run(table, 'post-delete', new PgPostDeleteEvent<T>(row));

			if (post_response.event.isCancelled()) {
				if (this._config.rollback_on_cancel) {
					throw new PgEventRollbackError(post_response.event.getCancelReason());
				}

				return {
					type: 'error',
					message: post_response.event.getCancelReason()
				};
			}

			return {
				type: 'success',
				data: row
			};
		};

		if (this._config.rollback_on_cancel) {
			try {
				return await this._database.transaction(async (tx) => runDelete(tx));
			} catch (error) {
				if (error instanceof PgEventRollbackError) {
					return {
						type: 'error',
						message: error.message
					};
				}

				return {
					type: 'error',
					message: 'An error occurred while updating the data.'
				};
			}
		}

		return await runDelete(this._database);
	}

	public async delete_batch<T extends PgTableWithColumns<any>>(
		table: T,
		primary_values: Array<
			InferSelectModel<T>[keyof InferSelectModel<T>] | Partial<InferSelectModel<T>>
		>
	): Promise<Response<InferSelectModel<T>[]>>;

	public async delete_batch<T extends PgTableWithColumns<any>>(
		table: T,
		primary_field: keyof InferSelectModel<T>,
		primary_values: Array<InferSelectModel<T>[keyof InferSelectModel<T>]>
	): Promise<Response<InferSelectModel<T>[]>>;

	public async delete_batch<T extends PgTableWithColumns<any>>(
		table: T,
		primary_field_or_values:
			| keyof InferSelectModel<T>
			| Array<InferSelectModel<T>[keyof InferSelectModel<T>] | Partial<InferSelectModel<T>>>,
		maybe_primary_values?: Array<InferSelectModel<T>[keyof InferSelectModel<T>]>
	): Promise<Response<InferSelectModel<T>[]>> {
		const primary_field =
			maybe_primary_values === undefined
				? undefined
				: (primary_field_or_values as keyof InferSelectModel<T>);
		const primary_values =
			maybe_primary_values === undefined
				? (primary_field_or_values as Array<
						InferSelectModel<T>[keyof InferSelectModel<T>] | Partial<InferSelectModel<T>>
					>)
				: maybe_primary_values;

		const primary_info = this._resolvePrimaryKeys(table, primary_field);

		if ('error' in primary_info) {
			return {
				type: 'error',
				message: `${primary_info.error} Pass a primary_field explicitly.`
			};
		}

		const runDeleteBatch = async (
			database: PgDatabase<any, any, any>,
			rollbackOnError: boolean
		): Promise<Response<InferSelectModel<T>[]>> => {
			const rows: InferSelectModel<T>[] = [];

			for (const primary_value of primary_values) {
				const where_result = this._buildWhereFromPrimaryValue(
					table,
					primary_info.keys,
					primary_value
				);

				if ('error' in where_result) {
					if (rollbackOnError) {
						throw new PgEventRollbackError(where_result.error);
					}

					return {
						type: 'error',
						message: where_result.error
					};
				}

				const [row] = (await database
					.select({
						...getTableColumns(table)
					})
					.from(table as PgTableWithColumns<any>)
					.where(where_result.where)) as InferSelectModel<T>[];

				if (!row) {
					const message = 'The row does not exist.';
					if (rollbackOnError) {
						throw new PgEventRollbackError(message);
					}

					return {
						type: 'error',
						message
					};
				}

				const pre_response = await this.run(table, 'pre-delete', new PgPreDeleteEvent<T>(row));

				if (pre_response.event.isCancelled()) {
					const message = pre_response.event.getCancelReason();
					if (rollbackOnError) {
						throw new PgEventRollbackError(message);
					}

					return {
						type: 'error',
						message
					};
				}

				try {
					await database.delete(table).where(where_result.where);
				} catch (error) {
					const message = 'An error occurred while updating the data.';
					if (rollbackOnError) {
						throw new PgEventRollbackError(message);
					}

					return {
						type: 'error',
						message
					};
				}

				const post_response = await this.run(table, 'post-delete', new PgPostDeleteEvent<T>(row));

				if (post_response.event.isCancelled()) {
					const message = post_response.event.getCancelReason();
					if (rollbackOnError) {
						throw new PgEventRollbackError(message);
					}

					return {
						type: 'error',
						message
					};
				}

				rows.push(row);
			}

			return {
				type: 'success',
				data: rows
			};
		};

		if (this._config.rollback_on_cancel) {
			try {
				return await this._database.transaction(async (tx) => runDeleteBatch(tx, true));
			} catch (error) {
				if (error instanceof PgEventRollbackError) {
					return {
						type: 'error',
						message: error.message
					};
				}

				return {
					type: 'error',
					message: 'An error occurred while updating the data.'
				};
			}
		}

		return await runDeleteBatch(this._database, false);
	}

	public put<
		T extends PgTableWithColumns<any>,
		E extends PgEventType,
		C extends PgEventClass<T, E>
	>(
		table: T,
		type: E,
		handler: (event: C) => Promise<void> | void,
		priority: EventPriority = EventPriority.NORMAL
	) {
		return this._register(this.getEventKey(table, type), handler, priority);
	}

	public async run<
		T extends PgTableWithColumns<any>,
		E extends PgEventType,
		C extends PgEventClass<T, E>
	>(table: T, type: E, event: C) {
		return await this._emit(this.getEventKey(table, type), event);
	}

	protected _resolvePrimaryKeys<T extends PgTableWithColumns<any>>(
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

		const table_columns = getTableColumns(table);
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

	protected _buildWhereFromPrimaryValue<T extends PgTableWithColumns<any>>(
		table: T,
		keys: (keyof InferSelectModel<T>)[],
		primary_value: unknown
	): { where: ReturnType<typeof eq> } | { error: string } {
		if (keys.length === 1) {
			const key = keys[0];
			const value =
				primary_value !== null &&
				typeof primary_value === 'object' &&
				key in (primary_value as Record<string, unknown>)
					? (primary_value as Record<string, unknown>)[key]
					: primary_value;

			return { where: eq(table[key], value as any) };
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

	protected _buildWhereFromKeys<T extends PgTableWithColumns<any>>(
		table: T,
		keys: (keyof InferSelectModel<T>)[],
		values: Partial<InferSelectModel<T>>
	) {
		const clauses = keys.map((key) => eq(table[key], values[key] as any));
		return clauses.length === 1 ? clauses[0] : and(...clauses);
	}

	private getEventKey(table: PgTableWithColumns<any>, type: PgEventType) {
		return getTableUniqueName(table) + ':' + type;
	}
}
