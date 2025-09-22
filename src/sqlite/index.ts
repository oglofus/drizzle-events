import {
    eq,
    getTableColumns,
    getTableUniqueName,
    InferInsertModel,
    InferSelectModel
} from "drizzle-orm";
import {CancellableEvent, EventPriority, RawEventManager} from "@oglofus/event-manager";
import {
    BaseSQLiteDatabase,
    SQLiteTableWithColumns,
    SQLiteUpdateSetSource
} from "drizzle-orm/sqlite-core";
import {
    deepMerge,
    DefaultEventManagerConfig,
    EventManagerConfig,
    PartialEventManagerConfig,
    Response
} from "../base/index.js";

export class SQLitePreInsertEvent<T extends SQLiteTableWithColumns<any>> extends CancellableEvent {
    private readonly _data: InferInsertModel<T>;

    constructor(data: InferInsertModel<T>) {
        super();

        this._data = data;
    }

    get data(): InferInsertModel<T> {
        return this._data;
    }
}

export class SQLitePostInsertEvent<T extends SQLiteTableWithColumns<any>> extends CancellableEvent {
    private readonly _row: InferSelectModel<T>;

    constructor(data: InferSelectModel<T>) {
        super();

        this._row = data;
    }

    get row(): InferSelectModel<T> {
        return this._row;
    }
}

export class SQLitePreUpdateEvent<T extends SQLiteTableWithColumns<any>> extends CancellableEvent {
    private readonly _data: SQLiteUpdateSetSource<T>;
    private readonly _row: InferSelectModel<T>;

    constructor(data: SQLiteUpdateSetSource<T>, row: InferSelectModel<T>) {
        super();

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

export class SQLitePostUpdateEvent<T extends SQLiteTableWithColumns<any>> extends CancellableEvent {
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

export class SQLitePreDeleteEvent<T extends SQLiteTableWithColumns<any>> extends CancellableEvent {
    private readonly _row: InferSelectModel<T>;

    constructor(row: InferSelectModel<T>) {
        super();

        this._row = row;
    }

    get row(): InferSelectModel<T> {
        return this._row;
    }
}

export class SQLitePostDeleteEvent<T extends SQLiteTableWithColumns<any>> extends CancellableEvent {
    private readonly _row: InferSelectModel<T>;

    constructor(row: InferSelectModel<T>) {
        super();

        this._row = row;
    }

    get row(): InferSelectModel<T> {
        return this._row;
    }
}

type SQLiteEventType =
    | 'pre-insert'
    | 'post-insert'
    | 'pre-update'
    | 'post-update'
    | 'pre-delete'
    | 'post-delete';

type SQLiteEventClass<T extends SQLiteTableWithColumns<any>, E extends SQLiteEventType> = E extends 'pre-insert'
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

export class SQLiteEventManager<TSchema extends Record<string, unknown> = Record<string, never>>
    extends RawEventManager {
    private readonly _config: EventManagerConfig;
    private readonly _database: BaseSQLiteDatabase<any, any, TSchema>;

    constructor(database: BaseSQLiteDatabase<any, any, TSchema>, config?: PartialEventManagerConfig) {
        super();
        this._database = database;
        this._config = deepMerge(DefaultEventManagerConfig, config ?? {});
    }

    get database(): BaseSQLiteDatabase<any, any, TSchema> {
        return this._database;
    }

    get config(): EventManagerConfig {
        return this._config;
    }

    public async insert<T extends SQLiteTableWithColumns<any>>(
        table: T,
        primary_field: keyof InferSelectModel<T>,
        data: InferInsertModel<T>
    ): Promise<Response<InferSelectModel<T>>> {
        const pre_response = await this.run(table, 'pre-insert', new SQLitePreInsertEvent<T>(data));

        if (pre_response.event.isCancelled()) {
            return {
                type: 'error',
                message: pre_response.event.getCancelReason()
            };
        }

        data = pre_response.event.data;
        let results: InferSelectModel<T>[];

        try {
            results = (await this._database
                                 .insert(table)
                                 .values(data)
                                 .returning()) as InferSelectModel<T>[];
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

        const post_response = await this.run(
            table,
            'post-insert',
            new SQLitePostInsertEvent<T>(row)
        );

        if (post_response.event.isCancelled()) {
            if (this._config.rollback_on_cancel) {
                await this._database
                          .delete(table)
                          .where(eq(table[primary_field], row[primary_field]))
                          .execute();
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
    }

    public async update<T extends SQLiteTableWithColumns<any>>(
        table: T,
        primary_field: keyof InferSelectModel<T>,
        primary_value: typeof primary_field extends keyof InferSelectModel<T>
                       ? InferSelectModel<T>[keyof InferSelectModel<T>]
                       : never,
        data: SQLiteUpdateSetSource<T>
    ): Promise<Response<InferSelectModel<T>>> {
        const old_row = await this._database
                                  .select({
                                      ...getTableColumns(table)
                                  })
                                  .from(table)
                                  .where(eq(table[primary_field], primary_value))
                                  .get();

        if (!old_row) {
            return {
                type: 'error',
                message: 'The row does not exist.'
            };
        }

        const pre_response = await this.run(
            table,
            'pre-update',
            new SQLitePreUpdateEvent<T>(data, old_row)
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
                data[key] = deepMerge(old_row[key], data[key])
            }
        }

        let results: InferSelectModel<T>[];

        try {
            results = (await this._database
                                 .update(table)
                                 .set(data)
                                 .where(eq(table[primary_field], primary_value))
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
            new SQLitePostUpdateEvent<T>(row, old_row)
        );

        if (post_response.event.isCancelled()) {
            if (this._config.rollback_on_cancel) {
                await this._database
                          .update(table)
                          .set(old_row)
                          .where(eq(table[primary_field], primary_value))
                          .execute();
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
    }

    public async delete<T extends SQLiteTableWithColumns<any>>(
        table: T,
        primary_field: keyof InferSelectModel<T>,
        primary_value: typeof primary_field extends keyof InferSelectModel<T>
                       ? InferSelectModel<T>[keyof InferSelectModel<T>]
                       : never
    ): Promise<Response<InferSelectModel<T>>> {
        const row = await this._database
                              .select({
                                  ...getTableColumns(table)
                              })
                              .from(table)
                              .where(eq(table[primary_field], primary_value))
                              .get();

        if (!row) {
            return {
                type: 'error',
                message: 'The row does not exist.'
            };
        }

        const pre_response = await this.run(table, 'pre-delete', new SQLitePreDeleteEvent<T>(row));

        if (pre_response.event.isCancelled()) {
            return {
                type: 'error',
                message: pre_response.event.getCancelReason()
            };
        }

        try {
            await this._database.delete(table).where(eq(table[primary_field], primary_value));
        } catch (error) {
            return {
                type: 'error',
                message: 'An error occurred while updating the data.'
            };
        }

        const post_response = await this.run(
            table,
            'post-delete',
            new SQLitePostDeleteEvent<T>(row)
        );

        if (post_response.event.isCancelled()) {
            if (this._config.rollback_on_cancel) {
                await this._database.insert(table).values(row).execute();
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
    }

    public put<T extends SQLiteTableWithColumns<any>, E extends SQLiteEventType, C extends SQLiteEventClass<T, E>>(
        table: T,
        type: E,
        handler: (event: C) => Promise<void> | void,
        priority: EventPriority = EventPriority.NORMAL
    )
    {
        return this._register(this.getEventKey(table, type), handler, priority);
    }

    public async run<T extends SQLiteTableWithColumns<any>, E extends SQLiteEventType, C extends SQLiteEventClass<T, E>>(
        table: T,
        type: E,
        event: C
    )
    {
        return await this._emit(this.getEventKey(table, type), event);
    }

    private getEventKey(table: SQLiteTableWithColumns<any>, type: SQLiteEventType) {
        return getTableUniqueName(table) + ':' + type;
    }
}
