export type Response<T> =
	| {
			type: 'success';
			data: T;
	  }
	| {
			type: 'error';
			message?: string | undefined;
	  };

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
