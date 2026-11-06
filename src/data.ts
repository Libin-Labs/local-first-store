export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export class LocalStoreError extends Error {
  constructor(
    readonly code: 'invalid' | 'capacity' | 'conflict' | 'adapter' | 'busy' | 'state' | 'observer',
    message: string,
  ) {
    super(message);
    this.name = 'LocalStoreError';
  }
}
export function fields(
  value: unknown,
  allowed?: readonly string[],
): asserts value is Record<string, unknown> {
  if (
    !value ||
    typeof value !== 'object' ||
    Array.isArray(value) ||
    ![Object.prototype, null].includes(Object.getPrototypeOf(value))
  )
    throw new LocalStoreError('invalid', 'Expected a plain data object.');
  const keys = Reflect.ownKeys(value);
  if (keys.length > 10_000) throw new LocalStoreError('capacity', 'Object field limit.');
  for (const key of keys) {
    const d = typeof key === 'string' ? Object.getOwnPropertyDescriptor(value, key) : undefined;
    if (
      typeof key !== 'string' ||
      !d?.enumerable ||
      !('value' in d) ||
      ['__proto__', 'constructor', 'prototype'].includes(key) ||
      (allowed && !allowed.includes(key))
    )
      throw new LocalStoreError('invalid', 'Unsupported field, prototype key or accessor.');
  }
}
export function integer(value: number, min = 1, max = 100_000): number {
  if (!Number.isSafeInteger(value) || value < min || value > max)
    throw new LocalStoreError('invalid', `Expected integer in ${min}..${max}.`);
  return value;
}
export function text(value: unknown): asserts value is string {
  if (
    typeof value !== 'string' ||
    value.length < 1 ||
    value.length > 256 ||
    ['__proto__', 'constructor', 'prototype'].includes(value)
  )
    throw new LocalStoreError('invalid', 'Expected safe identifier of 1..256 characters.');
}
export function copy(value: unknown): Json {
  let nodes = 0;
  let chars = 0;
  const visit = (v: unknown, depth: number): Json => {
    if (++nodes > 10_000 || depth > 32)
      throw new LocalStoreError('capacity', 'JSON structural limit.');
    if (v === null || typeof v === 'boolean') return v;
    if (typeof v === 'number' && Number.isFinite(v)) return v;
    if (typeof v === 'string') {
      chars += v.length;
      if (chars > 1_000_000) throw new LocalStoreError('capacity', 'JSON text limit.');
      return v;
    }
    if (Array.isArray(v)) {
      if (v.length > 10_000 || Reflect.ownKeys(v).length !== v.length + 1)
        throw new LocalStoreError('invalid', 'Expected dense unadorned array.');
      return Array.from({ length: v.length }, (_, i) => {
        const d = Object.getOwnPropertyDescriptor(v, String(i));
        if (!d?.enumerable || !('value' in d))
          throw new LocalStoreError('invalid', 'Array accessor or hole.');
        return visit(d.value, depth + 1);
      });
    }
    fields(v);
    return Object.fromEntries(
      Object.keys(v)
        .sort()
        .map((key) => {
          chars += key.length;
          if (key.length > 256 || chars > 1_000_000)
            throw new LocalStoreError('capacity', 'JSON key/text limit.');
          return [key, visit(v[key], depth + 1)];
        }),
    );
  };
  return visit(value, 0);
}
