import { copy, fields, integer, text, LocalStoreError, type Json } from './data.js';
export { LocalStoreError } from './data.js';
export type { Json } from './data.js';

export interface LocalRecord {
  key: string;
  value: Json;
  deleted: boolean;
  localRevision: number;
  remoteRevision: string | null;
  sync: 'pending' | 'synced' | 'conflict';
}
export interface LocalSnapshot {
  schemaVersion: number;
  revision: number;
  records: LocalRecord[];
}
export interface LocalPersistenceAdapter {
  load(): PromiseLike<LocalSnapshot | null>;
  save(snapshot: LocalSnapshot, expectedRevision: number | null): PromiseLike<boolean>;
}
export interface LocalStoreOptions {
  schemaVersion?: number;
  maxRecords?: number;
  maxSubscribers?: number;
  maxObserverErrors?: number;
  migrations?: Record<string, (snapshot: LocalSnapshot) => LocalSnapshot>;
}
export interface ObserverFailure {
  subscriber: number;
  message: string;
}
export interface LocalFirstStore {
  set(key: string, value: Json): Promise<LocalRecord>;
  delete(key: string): Promise<LocalRecord>;
  markSynced(key: string, localRevision: number, remoteRevision: string): Promise<LocalRecord>;
  markConflict(key: string, localRevision: number, remoteRevision: string): Promise<LocalRecord>;
  get(key: string): Json | undefined;
  snapshot(): LocalSnapshot;
  query<T extends Json>(select: (records: readonly LocalRecord[]) => T): T;
  subscribe<T extends Json>(
    select: (records: readonly LocalRecord[]) => T,
    listener: (result: T) => void,
  ): () => void;
  reload(): Promise<void>;
  close(): void;
  takeObserverErrors(): ObserverFailure[];
  status(): {
    phase: 'ready' | 'saving' | 'loading' | 'error' | 'closed';
    lastError: string | null;
    observerErrorCount: number;
    droppedObserverErrors: number;
  };
}
export async function openLocalFirstStore(
  adapter: LocalPersistenceAdapter,
  options: LocalStoreOptions = {},
): Promise<LocalFirstStore> {
  if (!adapter || typeof adapter.load !== 'function' || typeof adapter.save !== 'function')
    throw new LocalStoreError('invalid', 'Persistence load and atomic CAS save are required.');
  fields(options as unknown, [
    'schemaVersion',
    'maxRecords',
    'maxSubscribers',
    'maxObserverErrors',
    'migrations',
  ]);
  for (const name of [
    'schemaVersion',
    'maxRecords',
    'maxSubscribers',
    'maxObserverErrors',
  ] as const)
    if (options[name] !== undefined && typeof options[name] !== 'number')
      throw new LocalStoreError('invalid', 'Numeric options must be numbers.');
  if (options.migrations !== undefined) fields(options.migrations);
  const schemaVersion = integer(options.schemaVersion ?? 1, 1, 1000);
  const maxRecords = integer(options.maxRecords ?? 1000, 1, 10_000);
  const maxSubscribers = integer(options.maxSubscribers ?? 100, 1, 1000);
  const maxObserverErrors = integer(options.maxObserverErrors ?? 100, 1, 1000);
  const migrations = options.migrations ?? {};
  fields(migrations);
  for (const [version, migration] of Object.entries(migrations)) {
    if (
      !/^[1-9]\d*$/.test(version) ||
      Number(version) >= schemaVersion ||
      typeof migration !== 'function'
    )
      throw new LocalStoreError(
        'invalid',
        'Migrations must map an older schema number to a function.',
      );
  }
  const validate = (input: unknown): LocalSnapshot => {
    const state = copy(input) as unknown as LocalSnapshot;
    fields(state, ['schemaVersion', 'revision', 'records']);
    integer(state.schemaVersion, 1, schemaVersion);
    integer(state.revision, 0, Number.MAX_SAFE_INTEGER);
    if (!Array.isArray(state.records) || state.records.length > maxRecords)
      throw new LocalStoreError('capacity', 'Record limit includes tombstones.');
    const keys = new Set<string>();
    for (const record of state.records) {
      fields(record, ['key', 'value', 'deleted', 'localRevision', 'remoteRevision', 'sync']);
      text(record.key);
      integer(record.localRevision, 0, state.revision);
      if (record.remoteRevision !== null) text(record.remoteRevision);
      if (
        keys.has(record.key) ||
        typeof record.deleted !== 'boolean' ||
        !Object.hasOwn(record, 'value') ||
        (record.deleted && record.value !== null) ||
        !['pending', 'synced', 'conflict'].includes(record.sync)
      )
        throw new LocalStoreError('adapter', 'Invalid or duplicate local record.');
      keys.add(record.key);
    }
    state.records.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
    return state;
  };
  const load = async (): Promise<{ state: LocalSnapshot; expected: number | null }> => {
    const input = await adapter.load();
    if (input === null)
      return { state: { schemaVersion, revision: 0, records: [] }, expected: null };
    let state = validate(input);
    const expected = state.revision;
    const startVersion = state.schemaVersion;
    while (state.schemaVersion < schemaVersion) {
      const from = state.schemaVersion;
      const migration = migrations[String(from)];
      if (!migration) throw new LocalStoreError('state', `Missing migration from schema ${from}.`);
      state = validate(migration(validate(state)));
      if (state.schemaVersion !== from + 1 || state.revision !== expected)
        throw new LocalStoreError(
          'state',
          'Migration must advance one schema and preserve revision.',
        );
    }
    if (startVersion !== schemaVersion) {
      state.revision = integer(expected + 1, 1, Number.MAX_SAFE_INTEGER);
      const saved = await adapter.save(validate(state), expected);
      if (saved !== true)
        throw new LocalStoreError(
          saved === false ? 'conflict' : 'adapter',
          'Migration was not committed.',
        );
    }
    return { state, expected: state.revision };
  };
  const initial = await load();
  let current = initial.state;
  let expected = initial.expected;
  let phase: ReturnType<LocalFirstStore['status']>['phase'] = 'ready';
  let lastError: string | null = null;
  let sequence = 0;
  let dropped = 0;
  let delivering = false;
  const failures: ObserverFailure[] = [];
  const subscribers = new Map<
    number,
    {
      select: (records: readonly LocalRecord[]) => Json;
      listener: (result: Json) => void;
      previous: string;
    }
  >();
  const describe = (error: unknown): string =>
    error instanceof Error ? error.message.slice(0, 1024) : 'Operation failed.';
  const readable = () => {
    if (phase === 'closed') throw new LocalStoreError('state', 'Store is closed.');
  };
  const writable = () => {
    readable();
    if (delivering)
      throw new LocalStoreError('busy', 'Cannot write from a query or observer callback.');
    if (phase === 'saving' || phase === 'loading')
      throw new LocalStoreError('busy', 'A persistence operation is in flight.');
    if (phase === 'error')
      throw new LocalStoreError('state', 'Reload is required after a persistence failure.');
  };
  const select = <T extends Json>(query: (records: readonly LocalRecord[]) => T): T => {
    readable();
    if (typeof query !== 'function')
      throw new LocalStoreError('invalid', 'Query must be a function.');
    const previous = delivering;
    delivering = true;
    try {
      return copy(query(validate(current).records)) as T;
    } finally {
      delivering = previous;
    }
  };
  const notify = () => {
    const previous = delivering;
    delivering = true;
    try {
      for (const [id, subscriber] of [...subscribers]) {
        if (!subscribers.has(id)) continue;
        try {
          const value = select(subscriber.select);
          const signature = JSON.stringify(value);
          if (signature !== subscriber.previous) {
            subscriber.previous = signature;
            subscriber.listener(value);
          }
        } catch (error: unknown) {
          subscribers.delete(id);
          if (failures.length >= maxObserverErrors) {
            failures.shift();
            dropped++;
          }
          failures.push({ subscriber: id, message: describe(error) });
        }
      }
    } finally {
      delivering = previous;
    }
  };
  const persist = async (candidate: LocalSnapshot, key: string): Promise<LocalRecord> => {
    writable();
    const next = validate(candidate);
    const previous = current;
    phase = 'saving';
    lastError = null;
    current = next;
    notify();
    try {
      const saved = await adapter.save(validate(next), expected);
      if (saved !== true)
        throw new LocalStoreError(
          saved === false ? 'conflict' : 'adapter',
          'Snapshot was not committed.',
        );
      expected = next.revision;
      phase = 'ready';
      return copy(current.records.find((record) => record.key === key)) as unknown as LocalRecord;
    } catch (error: unknown) {
      current = previous;
      phase = 'error';
      lastError = describe(error);
      notify();
      throw error;
    }
  };
  const write = (key: string, value: Json, deleted: boolean): Promise<LocalRecord> => {
    writable();
    text(key);
    const data = copy(value);
    const candidate = validate(current);
    candidate.revision = integer(candidate.revision + 1, 1, Number.MAX_SAFE_INTEGER);
    let record = candidate.records.find((item) => item.key === key);
    if (!record) {
      if (candidate.records.length >= maxRecords)
        throw new LocalStoreError('capacity', 'Record limit includes tombstones.');
      record = {
        key,
        value: null,
        deleted: false,
        localRevision: 0,
        remoteRevision: null,
        sync: 'synced',
      };
      candidate.records.push(record);
    }
    record.value = data;
    record.deleted = deleted;
    record.localRevision = candidate.revision;
    record.sync = 'pending';
    return persist(candidate, key);
  };
  const metadata = (
    key: string,
    localRevision: number,
    remoteRevision: string,
    sync: 'synced' | 'conflict',
  ): Promise<LocalRecord> => {
    writable();
    text(key);
    integer(localRevision, 0, Number.MAX_SAFE_INTEGER);
    text(remoteRevision);
    const candidate = validate(current);
    const record = candidate.records.find((item) => item.key === key);
    if (!record || record.localRevision !== localRevision)
      throw new LocalStoreError(
        'conflict',
        'Sync metadata targets a stale or missing local revision.',
      );
    candidate.revision = integer(candidate.revision + 1, 1, Number.MAX_SAFE_INTEGER);
    record.remoteRevision = remoteRevision;
    record.sync = sync;
    return persist(candidate, key);
  };
  return {
    set: (key, value) => write(key, value, false),
    delete: (key) => write(key, null, true),
    markSynced: (key, revision, remoteRevision) =>
      metadata(key, revision, remoteRevision, 'synced'),
    markConflict: (key, revision, remoteRevision) =>
      metadata(key, revision, remoteRevision, 'conflict'),
    get(key) {
      readable();
      text(key);
      const record = current.records.find((item) => item.key === key);
      return !record || record.deleted ? undefined : copy(record.value);
    },
    snapshot: () => {
      readable();
      return validate(current);
    },
    query: select,
    subscribe(query, listener) {
      readable();
      if (delivering)
        throw new LocalStoreError('busy', 'Cannot subscribe inside observer delivery.');
      if (typeof listener !== 'function')
        throw new LocalStoreError('invalid', 'Listener must be a function.');
      if (subscribers.size >= maxSubscribers)
        throw new LocalStoreError('capacity', 'Subscriber limit reached.');
      const value = select(query);
      const signature = JSON.stringify(value);
      delivering = true;
      try {
        listener(value);
      } finally {
        delivering = false;
      }
      readable();
      sequence = integer(sequence + 1, 1, Number.MAX_SAFE_INTEGER);
      const id = sequence;
      subscribers.set(id, {
        select: query,
        listener: listener as (result: Json) => void,
        previous: signature,
      });
      return () => {
        subscribers.delete(id);
      };
    },
    async reload() {
      readable();
      if (delivering) throw new LocalStoreError('busy', 'Cannot reload inside observer delivery.');
      if (phase === 'saving' || phase === 'loading')
        throw new LocalStoreError('busy', 'Persistence operation is in flight.');
      phase = 'loading';
      try {
        const result = await load();
        current = result.state;
        expected = result.expected;
        phase = 'ready';
        lastError = null;
        notify();
      } catch (error: unknown) {
        phase = 'error';
        lastError = describe(error);
        throw error;
      }
    },
    close() {
      readable();
      if (delivering) throw new LocalStoreError('busy', 'Cannot close inside observer delivery.');
      if (phase === 'saving' || phase === 'loading')
        throw new LocalStoreError('busy', 'Cannot close during persistence.');
      subscribers.clear();
      phase = 'closed';
    },
    takeObserverErrors: () => failures.splice(0),
    status: () => ({
      phase,
      lastError,
      observerErrorCount: failures.length,
      droppedObserverErrors: dropped,
    }),
  };
}
