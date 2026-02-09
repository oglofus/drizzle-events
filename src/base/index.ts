import { CancellableEvent } from '@oglofus/event-manager';

export type PathSegment =
	| {
			readonly key: PropertyKey;
	  }
	| {
			readonly index: number;
	  };

export interface Issue {
	readonly message: string;
	readonly path?: ReadonlyArray<PropertyKey | PathSegment> | undefined;
}

export type IssueFactory<TSchema extends object> = {
	[K in Extract<keyof TSchema, string>]-?: (message: string) => Issue;
} & {
	$path: (path: ReadonlyArray<PropertyKey | PathSegment>, message: string) => Issue;
	$root: (message: string) => Issue;
};

export function createIssue(
	message: string,
	path?: ReadonlyArray<PropertyKey | PathSegment>
): Issue {
	if (path === undefined || path.length === 0) {
		return { message };
	}

	return {
		message,
		path: [...path]
	};
}

export function createIssueFactory<TSchema extends object>(
	fields: readonly Extract<keyof TSchema, string>[]
): IssueFactory<TSchema> {
	const factory: Record<string, any> & {
		$path: (path: ReadonlyArray<PropertyKey | PathSegment>, message: string) => Issue;
		$root: (message: string) => Issue;
	} = {
		$path: (path, message) => createIssue(message, path),
		$root: (message) => createIssue(message)
	};

	for (const field of fields) {
		factory[field] = (message: string) => createIssue(message, [field]);
	}

	return factory as IssueFactory<TSchema>;
}

export abstract class IssueEvent<TSchema extends object> extends CancellableEvent {
	private readonly _issues: Issue[] = [];
	private readonly _issue: IssueFactory<TSchema>;

	constructor(fields: readonly Extract<keyof TSchema, string>[]) {
		super();

		this._issue = createIssueFactory<TSchema>(fields);
		this.cancel = this.cancel.bind(this);
	}

	get issues(): ReadonlyArray<Issue> {
		return this._issues;
	}

	get issue(): IssueFactory<TSchema> {
		return this._issue;
	}

	public addIssues(...issues: Array<Issue | undefined>): void {
		for (const issue of issues) {
			if (!issue) {
				continue;
			}

			this._issues.push(issue);
		}
	}

	public cancel(reason?: string): void;
	public cancel(...issues: Issue[]): void;
	public cancel(reason: string, ...issues: Issue[]): void;
	public override cancel(reason_or_issue?: string | Issue, ...issues: Issue[]): void {
		let reason = '';

		if (typeof reason_or_issue === 'string') {
			reason = reason_or_issue;
			this.addIssues(...issues);
		} else if (reason_or_issue === undefined) {
			this.addIssues(...issues);
		} else {
			this.addIssues(reason_or_issue, ...issues);
			reason = reason_or_issue.message;
		}

		super.cancel(reason);
	}
}

export type Response<T> =
	| {
			type: 'success';
			data: T;
			issues: Issue[];
	  }
	| {
			type: 'error';
			message?: string | undefined;
			issues: Issue[];
	  };

export const successResponse = <T>(data: T, issues: Issue[] = []): Response<T> => ({
	type: 'success',
	data,
	issues
});

export const errorResponse = <T = never>(message?: string, issues: Issue[] = []): Response<T> => ({
	type: 'error',
	message,
	issues
});

export type ArrayStrategy = 'replace' | 'concat' | 'union';

export type EventManagerConfig = {
	merge_objects: boolean;
	array_strategy: ArrayStrategy;
	rollback_on_cancel: boolean;
};

export type PartialEventManagerConfig = Partial<EventManagerConfig>;

export const DefaultEventManagerConfig: EventManagerConfig = {
	merge_objects: true,
	array_strategy: 'union',
	rollback_on_cancel: true
};

export type Primitive = string | number | boolean | symbol | bigint | null | undefined;

export type IsPlainObject<T> = T extends Primitive
	? false
	: T extends Function
		? false
		: T extends readonly any[]
			? false
			: T extends object
				? true
				: false;

export type MergeDeep<T, U> = {
	[K in keyof T | keyof U]: K extends keyof U
		? K extends keyof T
			? IsPlainObject<T[K]> extends true
				? IsPlainObject<U[K]> extends true
					? MergeDeep<T[K], U[K]>
					: U[K]
				: U[K]
			: U[K]
		: K extends keyof T
			? T[K]
			: never;
};

export const isPlainObject = (value: unknown): value is Record<string, unknown> =>
	Object.prototype.toString.call(value) === '[object Object]';

export function deepMerge<T, U>(
	a: T,
	b: U,
	array_strategy: ArrayStrategy = 'union'
): MergeDeep<T, U> {
	if (!isPlainObject(a)) {
		return b as MergeDeep<T, U>;
	}

	if (!isPlainObject(b)) {
		return b as MergeDeep<T, U>;
	}

	const out: Record<string, unknown> = { ...(a as Record<string, unknown>) };

	for (const [key, b_value] of Object.entries(b)) {
		const a_value = (a as Record<string, unknown>)[key];

		if (isPlainObject(a_value) && isPlainObject(b_value)) {
			out[key] = deepMerge(a_value, b_value, array_strategy);

			continue;
		}

		if (Array.isArray(a_value) && Array.isArray(b_value)) {
			if (array_strategy === 'concat') {
				out[key] = [...a_value, ...b_value];
			} else if (array_strategy === 'union') {
				out[key] = Array.from(new Set([...a_value, ...b_value]));
			} else {
				out[key] = [...b_value];
			}

			continue;
		}

		if (Array.isArray(b_value)) {
			out[key] = [...b_value];
		} else if (isPlainObject(b_value)) {
			out[key] = { ...b_value };
		} else {
			out[key] = b_value;
		}
	}

	return out as MergeDeep<T, U>;
}

export abstract class DatabaseManager {}
