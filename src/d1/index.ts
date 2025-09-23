import { eq, InferInsertModel, InferSelectModel } from 'drizzle-orm';
import { SQLiteTableWithColumns } from 'drizzle-orm/sqlite-core';
import { PartialEventManagerConfig, Response } from '../base/index.js';
import {
	SQLiteEventManager,
	SQLitePostInsertEvent,
	SQLitePreInsertEvent
} from '../sqlite/index.js';
import { DrizzleD1Database } from 'drizzle-orm/d1';

export class D1EventManager<D extends DrizzleD1Database<any>> extends SQLiteEventManager<D> {
	constructor(database: D, config?: PartialEventManagerConfig) {
		super(database, config);
	}

	public async insertBatch<T extends SQLiteTableWithColumns<any>>(
		table: T,
		primary_field: keyof InferSelectModel<T>,
		data: InferInsertModel<T>[]
	): Promise<Response<InferSelectModel<T>[]>> {
		for (let i = 0; i < data.length; i++) {
			const pre_response = await this.run(
				table,
				'pre-insert',
				new SQLitePreInsertEvent<T>(data[i])
			);

			if (pre_response.event.isCancelled()) {
				return {
					type: 'error',
					message: pre_response.event.getCancelReason()
				};
			}

			data[i] = pre_response.event.data;
		}

		let results: InferSelectModel<T>[];

		try {
			results = (await this._database.batch(
				data.map((r) => this._database.insert(table).values(r).returning()) as any
			)) as any as InferSelectModel<T>[];
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

		for (let i = 0; i < results.length; i++) {
			const post_response = await this.run(
				table,
				'post-insert',
				new SQLitePostInsertEvent<T>(results[i])
			);

			if (post_response.event.isCancelled()) {
				for (let j = 0; j < results.length; j++) {
					await this._database
						.delete(table)
						.where(eq(table[primary_field], results[j][primary_field]))
						.execute();
				}

				return {
					type: 'error',
					message: post_response.event.getCancelReason()
				};
			}
		}

		return {
			type: 'success',
			data: results
		};
	}
}
