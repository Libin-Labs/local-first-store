import test from 'node:test';
import assert from 'node:assert/strict';
import { openLocalFirstStore, LocalStoreError } from '../dist/esm/index.js';
import { fileAdapter } from './file-adapter.mjs';
const count = (records) => records.filter((row) => !row.deleted).length;

test('optimistic write is immediately visible before file persistence completes', async () => {
  const backend = await fileAdapter('optimistic');
  let release;
  let entered;
  const started = new Promise((resolve) => {
    entered = resolve;
  });
  const blocked = new Promise((resolve) => {
    release = resolve;
  });
  const store = await openLocalFirstStore({
    load: () => backend.adapter.load(),
    save: async (...args) => {
      entered();
      await blocked;
      return backend.adapter.save(...args);
    },
  });
  const events = [];
  store.subscribe(count, (value) => events.push(value));
  const pending = store.set('key', { text: 'local' });
  await started;
  assert.deepEqual(store.get('key'), { text: 'local' });
  assert.equal(store.status().phase, 'saving');
  assert.throws(() => store.set('other', 1), { code: 'busy' });
  assert.throws(() => store.close(), { code: 'busy' });
  release();
  const record = await pending;
  assert.equal(record.sync, 'pending');
  assert.equal(record.localRevision, 1);
  assert.deepEqual(events, [0, 1]);
  assert.equal(store.status().phase, 'ready');
});
test('save failure rolls back visible values and queries, blocks writes until reload', async () => {
  const backend = await fileAdapter('rollback');
  const error = new Error('disk full');
  const store = await openLocalFirstStore({
    load: () => backend.adapter.load(),
    save: async () => {
      throw error;
    },
  });
  const values = [];
  store.subscribe(count, (value) => values.push(value));
  await assert.rejects(store.set('x', 1), (e) => e === error);
  assert.equal(store.get('x'), undefined);
  assert.deepEqual(values, [0, 1, 0]);
  assert.deepEqual(store.status(), {
    phase: 'error',
    lastError: 'disk full',
    observerErrorCount: 0,
    droppedObserverErrors: 0,
  });
  assert.throws(() => store.set('y', 2), { code: 'state' });
  await store.reload();
  assert.equal(store.status().phase, 'ready');
});
test('persisted values tombstones and metadata survive reopening', async () => {
  const backend = await fileAdapter('restart');
  const first = await openLocalFirstStore(backend.adapter);
  await first.set('alive', { n: 1 });
  const tombstone = await first.delete('gone');
  await first.markSynced('gone', tombstone.localRevision, 'r2');
  const second = await openLocalFirstStore(backend.reopen());
  assert.deepEqual(second.get('alive'), { n: 1 });
  assert.equal(second.get('gone'), undefined);
  assert.equal(second.snapshot().records[1].deleted, true);
  assert.equal(second.snapshot().records[1].sync, 'synced');
  assert.equal(second.snapshot().revision, 3);
});
test('CAS races across two stores accept only one snapshot and loser reloads', async () => {
  const backend = await fileAdapter('cas-race');
  const a = await openLocalFirstStore(backend.adapter);
  const b = await openLocalFirstStore(backend.reopen());
  const results = await Promise.allSettled([a.set('a', 1), b.set('b', 2)]);
  assert.equal(results.filter((result) => result.status === 'fulfilled').length, 1);
  const rejected = results.find((result) => result.status === 'rejected');
  assert.equal(rejected.reason.code, 'conflict');
  const loser = a.status().phase === 'error' ? a : b;
  assert.deepEqual(loser.snapshot().records, []);
  await loser.reload();
  assert.equal(loser.snapshot().records.length, 1);
});
test('sync metadata rejects stale revision and writes preserve remote revision', async () => {
  const backend = await fileAdapter('metadata');
  const store = await openLocalFirstStore(backend.adapter);
  const first = await store.set('x', 1);
  await store.markSynced('x', first.localRevision, 'remote-1');
  const next = await store.set('x', 2);
  assert.equal(next.remoteRevision, 'remote-1');
  assert.equal(next.sync, 'pending');
  assert.throws(() => store.markSynced('x', first.localRevision, 'stale'), { code: 'conflict' });
  const conflict = await store.markConflict('x', next.localRevision, 'remote-2');
  assert.equal(conflict.sync, 'conflict');
  assert.equal(conflict.value, 2);
});
test('reactive queries dedupe structural results and unsubscribe', async () => {
  const backend = await fileAdapter('reactive');
  const store = await openLocalFirstStore(backend.adapter);
  const values = [];
  const unsubscribe = store.subscribe(count, (value) => values.push(value));
  await store.set('x', 1);
  await store.set('x', 2);
  await store.delete('x');
  assert.deepEqual(values, [0, 1, 0]);
  unsubscribe();
  await store.set('y', 3);
  assert.deepEqual(values, [0, 1, 0]);
});
test('query inputs, listeners, adapter and returned values cannot mutate owned state', async () => {
  const backend = await fileAdapter('ownership');
  const store = await openLocalFirstStore(backend.adapter);
  const input = { arr: [1] };
  const promise = store.set('x', input);
  input.arr.push(2);
  const result = await promise;
  result.value.arr.push(3);
  store.query((records) => {
    records[0].value.arr.push(4);
    return null;
  });
  const read = store.get('x');
  read.arr.push(5);
  const snapshot = store.snapshot();
  snapshot.records[0].value.arr.push(6);
  store.subscribe(
    (records) => records[0].value,
    (value) => value.arr.push(7),
  );
  assert.deepEqual(store.get('x'), { arr: [1] });
});
test('observer failures are bounded visible and removed, do not misreport committed writes', async () => {
  const backend = await fileAdapter('observer');
  const store = await openLocalFirstStore(backend.adapter, { maxObserverErrors: 1 });
  for (let i = 0; i < 2; i++)
    store.subscribe(count, (value) => {
      if (value) throw new Error(`observer ${i}`);
    });
  await store.set('x', 1);
  assert.equal(store.status().phase, 'ready');
  assert.equal(store.status().observerErrorCount, 1);
  assert.equal(store.status().droppedObserverErrors, 1);
  assert.deepEqual(store.takeObserverErrors(), [{ subscriber: 2, message: 'observer 1' }]);
  assert.equal(store.status().observerErrorCount, 0);
  await store.set('y', 2);
  assert.deepEqual(store.takeObserverErrors(), []);
});
test('initial observer errors propagate and do not occupy subscriber capacity', async () => {
  const backend = await fileAdapter('initial-observer');
  const store = await openLocalFirstStore(backend.adapter, { maxSubscribers: 1 });
  assert.throws(
    () =>
      store.subscribe(count, () => {
        throw new Error('initial');
      }),
    /initial/,
  );
  const unsubscribe = store.subscribe(count, () => {});
  assert.throws(() => store.subscribe(count, () => {}), { code: 'capacity' });
  unsubscribe();
  store.subscribe(count, () => {});
});
test('one-step migrations persist before publishing opened store', async () => {
  const backend = await fileAdapter('migration', {
    schemaVersion: 1,
    revision: 3,
    records: [
      {
        key: 'x',
        value: { old: 1 },
        deleted: false,
        localRevision: 3,
        remoteRevision: null,
        sync: 'pending',
      },
    ],
  });
  const store = await openLocalFirstStore(backend.adapter, {
    schemaVersion: 3,
    migrations: {
      1: (snapshot) => ({
        ...snapshot,
        schemaVersion: 2,
        records: snapshot.records.map((record) => ({
          ...record,
          value: { renamed: record.value.old },
        })),
      }),
      2: (snapshot) => ({ ...snapshot, schemaVersion: 3 }),
    },
  });
  assert.equal(store.snapshot().schemaVersion, 3);
  assert.equal(store.snapshot().revision, 4);
  assert.deepEqual(store.get('x'), { renamed: 1 });
  const reopened = await openLocalFirstStore(backend.reopen(), { schemaVersion: 3 });
  assert.deepEqual(reopened.snapshot(), store.snapshot());
});
test('missing invalid and failed migrations never overwrite old backend state', async () => {
  const backend = await fileAdapter('failed-migration', {
    schemaVersion: 1,
    revision: 0,
    records: [],
  });
  await assert.rejects(openLocalFirstStore(backend.adapter, { schemaVersion: 2 }), {
    code: 'state',
  });
  await assert.rejects(
    openLocalFirstStore(backend.adapter, {
      schemaVersion: 2,
      migrations: {
        1: (state) => ({ ...state, schemaVersion: 2, revision: 1 }),
      },
    }),
    { code: 'state' },
  );
  await assert.rejects(
    openLocalFirstStore(backend.adapter, {
      schemaVersion: 2,
      migrations: {
        1: () => {
          throw new Error('migration failed');
        },
      },
    }),
    /migration failed/,
  );
  assert.equal((await backend.adapter.load()).schemaVersion, 1);
});
test('migration CAS rejection is an explicit open failure', async () => {
  await assert.rejects(
    openLocalFirstStore(
      {
        load: async () => ({ schemaVersion: 1, revision: 0, records: [] }),
        save: async () => false,
      },
      { schemaVersion: 2, migrations: { 1: (s) => ({ ...s, schemaVersion: 2 }) } },
    ),
    { code: 'conflict' },
  );
});
test('record capacity counts retained tombstones; oversized JSON does not become visible', async () => {
  const backend = await fileAdapter('capacity');
  const store = await openLocalFirstStore(backend.adapter, { maxRecords: 1 });
  await store.delete('x');
  assert.throws(() => store.set('y', 2), { code: 'capacity' });
  assert.throws(() => store.set('x', 'x'.repeat(1_000_001)), { code: 'capacity' });
  assert.equal(store.get('x'), undefined);
  assert.equal(store.status().phase, 'ready');
});
test('malformed snapshots unsafe prototypes and accessors reject without invocation', async () => {
  let invoked = false;
  const backend = await fileAdapter('guards');
  const store = await openLocalFirstStore(backend.adapter);
  const value = Object.defineProperty({}, 'x', {
    enumerable: true,
    get() {
      invoked = true;
    },
  });
  assert.throws(() => store.set('x', value), LocalStoreError);
  assert.equal(invoked, false);
  for (const data of [NaN, new Date(), JSON.parse('{"constructor":1}'), [, 1]])
    assert.throws(() => store.set('x', data), LocalStoreError);
  await assert.rejects(
    openLocalFirstStore({
      load: async () => ({ schemaVersion: 1, revision: 0, records: 'bad' }),
      save: async () => true,
    }),
    LocalStoreError,
  );
});
test('reload errors retain known snapshot and close blocks operations', async () => {
  const backend = await fileAdapter('reload-failure');
  let fail = false;
  const store = await openLocalFirstStore({
    load: async () => {
      if (fail) throw new Error('load failed');
      return backend.adapter.load();
    },
    save: (...args) => backend.adapter.save(...args),
  });
  await store.set('x', 1);
  fail = true;
  await assert.rejects(store.reload(), /load failed/);
  assert.equal(store.get('x'), 1);
  store.close();
  assert.equal(store.status().phase, 'closed');
  assert.throws(() => store.snapshot(), { code: 'state' });
  await assert.rejects(store.reload(), { code: 'state' });
});
test('snapshot counter overflow rejects before optimistic publication', async () => {
  const backend = await fileAdapter('overflow', {
    schemaVersion: 1,
    revision: Number.MAX_SAFE_INTEGER,
    records: [],
  });
  const store = await openLocalFirstStore(backend.adapter);
  assert.throws(() => store.set('x', 1), LocalStoreError);
  assert.equal(store.snapshot().records.length, 0);
});
test('query and listener reentrant writes cannot corrupt provisional or committed state', async () => {
  const backend = await fileAdapter('reentrant');
  const store = await openLocalFirstStore(backend.adapter);
  assert.throws(
    () =>
      store.query(() => {
        store.set('hidden', 1);
        return null;
      }),
    { code: 'busy' },
  );
  assert.throws(() => store.subscribe(count, () => store.close()), { code: 'busy' });
  store.subscribe(count, (value) => {
    if (value) store.set('hidden', 2);
  });
  await store.set('visible', 1);
  assert.equal(store.get('hidden'), undefined);
  assert.equal(store.status().phase, 'ready');
  assert.equal(store.takeObserverErrors().length, 1);
  assert.equal((await backend.adapter.load()).records.length, 1);
});
test('adapter mutated snapshot and nonboolean success cannot silently modify state', async () => {
  const backend = await fileAdapter('adapter-mutation');
  const store = await openLocalFirstStore({
    load: () => backend.adapter.load(),
    save: async (state) => {
      state.records[0].value = 'tampered';
      return 'success';
    },
  });
  await assert.rejects(store.set('x', 1), { code: 'adapter' });
  assert.equal(store.get('x'), undefined);
  assert.equal(store.status().phase, 'error');
});
test('overlapping reloads and writes are bounded while persistence is loading', async () => {
  const backend = await fileAdapter('reload-race');
  let block = false;
  let release;
  let entered;
  const started = new Promise((resolve) => {
    entered = resolve;
  });
  const wait = new Promise((resolve) => {
    release = resolve;
  });
  const store = await openLocalFirstStore({
    load: async () => {
      if (block) {
        entered();
        await wait;
      }
      return backend.adapter.load();
    },
    save: (...args) => backend.adapter.save(...args),
  });
  block = true;
  const reload = store.reload();
  await started;
  assert.throws(() => store.set('x', 1), { code: 'busy' });
  await assert.rejects(store.reload(), { code: 'busy' });
  release();
  await reload;
  assert.equal(store.status().phase, 'ready');
});
test('null options and unsupported future schemas are explicit open errors', async () => {
  const backend = await fileAdapter('null-options', { schemaVersion: 2, revision: 0, records: [] });
  await assert.rejects(openLocalFirstStore(backend.adapter), LocalStoreError);
  await assert.rejects(
    openLocalFirstStore(backend.adapter, { schemaVersion: null }),
    LocalStoreError,
  );
  await assert.rejects(openLocalFirstStore(backend.adapter, { migrations: null }), LocalStoreError);
});
