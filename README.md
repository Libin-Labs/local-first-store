# @lp-labs/local-first-store

Original bounded optimistic JSON store: reactive queries, atomic persistence CAS, sequential
schema migrations, explicit rollback/error state and revision-fenced sync metadata. Version
0.1.0, strict NodeNext ESM/CommonJS declarations, Node 18+, zero runtime/peer dependencies.

**Local-first does not mean magically durable.** A real injected backend supplies persistence.
There is no bundled memory backend, hidden transport, implicit sync, or sibling import.

## Install and runnable file-backed example

```sh
npm install @lp-labs/local-first-store
```

This actual-file demonstration performs compare-and-swap under one adapter's process-local
queue, flushes bytes and atomically renames. It does not lock other processes/adapter instances
and does not guarantee power-loss durability of the directory entry. It resets only its named
demo file. Production requires the real persistence contract below.

```js
import assert from 'node:assert/strict';
import { open, readFile, rename, writeFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { openLocalFirstStore, LocalStoreError } from '@lp-labs/local-first-store';

async function replaceFile(source, destination) {
  for (let attempt = 0; attempt < 8; attempt++) {
    try {
      await rename(source, destination);
      return;
    } catch (error) {
      if (
        process.platform !== 'win32' ||
        !['EPERM', 'EACCES', 'EBUSY'].includes(error.code) ||
        attempt === 7
      )
        throw error;
      await delay(Math.min(25 * 2 ** attempt, 200));
    }
  }
}

const path = new URL('./local-example.json', import.meta.url);
const staging = new URL('./local-example.next', import.meta.url);
await writeFile(path, JSON.stringify({ schemaVersion: 1, revision: 0, records: [] }));
let gate = Promise.resolve();
const adapter = {
  async load() {
    await gate;
    return JSON.parse(await readFile(path, 'utf8'));
  },
  save(snapshot, expectedRevision) {
    const captured = structuredClone(snapshot);
    const operation = gate.then(async () => {
      const previous = JSON.parse(await readFile(path, 'utf8'));
      if ((previous?.revision ?? null) !== expectedRevision) return false;
      const file = await open(staging, 'w');
      try {
        await file.writeFile(JSON.stringify(captured));
        await file.sync();
      } finally {
        await file.close();
      }
      await replaceFile(staging, path);
      return true;
    });
    gate = operation.catch(() => undefined);
    return operation;
  },
};
const store = await openLocalFirstStore(adapter, {
  schemaVersion: 2,
  migrations: { 1: (snapshot) => ({ ...snapshot, schemaVersion: 2 }) },
});
const counts = [];
const stop = store.subscribe(
  (records) => records.filter((record) => !record.deleted).length,
  (count) => counts.push(count),
);
const pending = store.set('note', { title: 'Optimistic edit' });
assert.deepEqual(store.get('note'), { title: 'Optimistic edit' }); // Before awaiting save.
const record = await pending;
await store.markConflict('note', record.localRevision, 'remote-1');
await store.markSynced('note', record.localRevision, 'remote-2');
await store.delete('note');
assert.equal(
  store.query((records) => records.filter((record) => record.deleted).length),
  1,
);
assert.equal(store.snapshot().revision, 5);
assert.deepEqual(store.takeObserverErrors(), []);
stop();
await store.reload();
assert.equal(store.get('note'), undefined);
store.close();
assert.equal(store.status().phase, 'closed');
console.log(counts); // [0, 1, 0]
const error = new LocalStoreError('state', 'Reload before another write.');
assert.equal(error.code, 'state');
```

CommonJS: `const { openLocalFirstStore, LocalStoreError } = require('@lp-labs/local-first-store')`.

On Windows, replacement retries only `EPERM`, `EACCES`, and `EBUSY`, at most eight rename
attempts with seven waits totaling at most 975 ms. These codes can also indicate permanent
permissions problems: the final original error is rethrown; other errors/platforms fail immediately.
Each attempt uses same-directory rename without deleting the destination, keeping the previous
snapshot until replacement succeeds. Persistent locks still fail; syscall duration is not bounded
by the wait budget. This does not add cross-process locking, directory fsync, or a power-loss guarantee.

## Persistence and migrations

`LocalPersistenceAdapter`:

- `load() -> PromiseLike<LocalSnapshot | null>`: fresh isolated persisted snapshot; null only
  if absent. Missing file may map to null; I/O/parse errors must reject, not masquerade as absence.
- `save(snapshot, expectedRevision) -> PromiseLike<boolean>`: atomically compare current
  persisted revision (or null for absence), then replace the complete snapshot if equal.
  Resolve true **only after successful durable commit**; false means CAS mismatch with
  **no write**. Throw/reject on other failures, with no definite partial commit.

All writers to the same backend require shared locking/CAS, not separate process-local queues.
The library checks data, boolean results and clone ownership but cannot prove locking, flushing
or durability. Multiple stores can race: one wins CAS; the loser rolls back, enters error and
must reload. No silent last-writer-wins overwrite exists.

`openLocalFirstStore(adapter, options?) -> Promise<LocalFirstStore>` loads and validates before
returning a store. Null starts an empty in-memory working snapshot, persisted on first write.
Older schemas require every one-step migration; newer schemas reject. Each synchronous migration
receives an owned snapshot, must advance `schemaVersion` exactly one and preserve `revision`.
All steps validate bounds and schema. After all steps, one CAS save increments persisted revision
once; migration failure/CAS loss rejects opening and publishes no store. Migration functions must
be pure, deterministic and must not perform side effects; no runtime version switching is hidden.

`LocalSnapshot = { schemaVersion: number, revision: number, records: LocalRecord[] }`.
`LocalRecord = { key, value: Json, deleted, localRevision, remoteRevision: string | null,
sync: 'pending' | 'synced' | 'conflict' }`.
Snapshot revision is a global safe integer incremented on every write or metadata commit.
`localRevision` identifies the latest value mutation; metadata updates do not change it.
Records are sorted by key. A live null is distinct from a tombstone (`deleted: true, value: null`).

## API and customization

| `LocalStoreOptions` field | Default | Bounds                                                                 |
| ------------------------- | ------- | ---------------------------------------------------------------------- |
| `schemaVersion`           | 1       | Integer 1..1000                                                        |
| `maxRecords`              | 1000    | 1..10,000, including tombstones                                        |
| `maxSubscribers`          | 100     | 1..1000                                                                |
| `maxObserverErrors`       | 100     | 1..1000 retained diagnostics                                           |
| `migrations`              | `{}`    | Map of older integer version strings to synchronous snapshot functions |

Every `LocalFirstStore` method:

- `set(key, value) -> Promise<LocalRecord>`: validate and immediately publish an optimistic
  value with new localRevision and sync pending. Then CAS-persist. Promise resolves with an
  owned record only after success.
- `delete(key) -> Promise<LocalRecord>`: same optimistic transaction with a retained tombstone;
  deleting an absent key still allocates a record.
- `get(key) -> Json | undefined`: owned live JSON, absent/tombstone undefined; reads may see
  a currently optimistic uncommitted state.
- `snapshot() -> LocalSnapshot`: owned complete snapshot including tombstones and metadata.
- `query<T extends Json>(select: (records: readonly LocalRecord[]) => T) -> T`: synchronous
  selector over cloned records; output validated/cloned JSON. Does not store query results.
- `subscribe<T extends Json>(select, listener: (result: T) => void) -> () => void`: synchronously
  deliver initial selected value, then changed results after optimistic publication, rollback,
  metadata changes and reload. Structural equality uses canonical object keys/JSON strings;
  no duplicate notification for an identical result at commit. Unsubscribe is idempotent.
- `markSynced(key, localRevision, remoteRevision) -> Promise<LocalRecord>`: optimistic,
  persisted metadata update only if the latest value revision matches. Stale/missing key
  throws conflict. It does not send data or infer a server acknowledgment.
- `markConflict(key, localRevision, remoteRevision) -> Promise<LocalRecord>`: same fence,
  retain local data and explicitly mark conflict. It does not merge or adopt remote values.
- `reload() -> Promise<void>`: explicitly load/validate/migrate latest backend state and publish
  query changes on success. Permitted after an error. Failure retains known local snapshot
  and error phase.
- `close() -> void`: remove subscribers and enter closed. In-flight persistence or callback
  delivery prevents close with busy. Closed stores reject reads/writes/reload/subscription.
- `status() -> { phase, lastError, observerErrorCount, droppedObserverErrors }`: owned status,
  available even after close. Phase is ready, saving, loading, error or closed.
- `takeObserverErrors() -> ObserverFailure[]`: drain owned bounded diagnostics
  `{ subscriber: number, message: string }`, available even after close. Subscriber ids
  are monotonic safe integers; overflow rejects registration.

Queries and listeners are synchronous and should be pure/short. During selector/listener delivery,
write, metadata, reload, close and subscription registration are rejected with busy to prevent
reentrant state corruption. Unsubscribe and reads are allowed. Selectors receive mutable clones
but readonly array typing; mutating them cannot affect stored data. Async callbacks/results are
unsupported; promises returned as query results fail JSON validation.

## Optimism, failures and observer errors

Only **one persistence operation per store** may be in flight; concurrent writes/reload/close
reject busy rather than grow an unbounded queue. Revisions/JSON/capacity validation occurs
before optimistic publication. Once published, observers can read the candidate while saving.
On false CAS or rejected save, the entire prior local snapshot is restored, rollback query
notifications run, the original error rejects, and phase becomes error. Further writes require
successful reload; there is no silent retry/overwrite.

An ambiguous backend failure may have committed remotely: local rollback is not proof of backend
rollback. Reload reconciles that outcome. A caller may observe a provisional value that later
rolls back, so external irreversible side effects must wait for the write promise.

An initial query/listener exception propagates unchanged and does not register a subscriber.
Later selector/listener failures remove that subscriber and append an explicit bounded
`ObserverFailure`, without misreporting a committed persistence operation as failed.
`status().observerErrorCount` and `takeObserverErrors()` expose them; oldest diagnostics are
dropped on overflow, counted by `droppedObserverErrors`. Monitor/drain this channel; observer
exceptions are never silently swallowed or used to roll back successful writes.

## Limits, safety, performance and supported scope

`LocalStoreError` extends `Error`, `name = 'LocalStoreError'`; `code` is `invalid`, `capacity`,
`conflict`, `adapter`, `busy`, `state`, or `observer` (reserved for application observer errors).
Backend/migration/query exceptions retain identity. Validation may throw synchronously for
methods returning promises; open/reload/persistence failures reject.

Keys and remote revisions are safe nonempty 1..256-character strings. Every JSON clone is
bounded to 10,000 nodes, depth 32, 1,000,000 total key/string characters, 10,000 own object
keys and 256 characters per key. **The entire snapshot** is an aggregate bounded tree, so
large values can reach capacity before maxRecords. Dense arrays, finite numbers,
plain/null-prototype enumerable string data properties only. Accessors, holes, symbols,
class instances, undefined, cycles and `__proto__`, `constructor`, `prototype` keys reject,
without invoking getters. Hostile proxies are outside the contract. All returned data and
backend handoffs have clone ownership.

Tombstones always remain and consume record capacity: there is no unsafe deletion acknowledgment
or automatic purge. Pending/conflict metadata is durable only according to the adapter; it is
**not a persisted delivery outbox**. Store set/delete may supersede prior local pending values;
applications requiring every mutation to reach a server need a separate atomic outbox contract.
No networking, bidirectional sync, automatic conflict resolution, cross-device convergence,
indexed queries, transaction batches or exactly-once delivery is promised.

Each write clones/validates the complete bounded snapshot and does one CAS save. Each query
clones records; each publication reruns up to maxSubscribers selectors, with structural
dedupe and canonical key sorting. Migration steps are at most 999. Callback execution time
cannot be bounded; the library favors explicit small-store behavior over database-scale throughput.

## Development

```sh
npm ci --offline
npm run build
npm run typecheck
npm test
npm run format:check
```

`build` emits ESM/CJS with declarations; `typecheck` compiles bare `.mts`/`.cts` consumers;
`test` builds and runs standalone `../dist/esm/index.js` tests, including actual-file reopen,
CAS races, rollback and migrations. Test snapshots are retained. `format` writes Prettier;
`prepack` builds. TypeScript ^5.9.3 and Prettier ^3.6.2 are dev-only.

## Support

If this library is useful to you, consider supporting its maintenance with a [donation via PayPal](https://www.paypal.me/LibinPrasanth).

## Author

Created by [Libin Prasanth](https://www.linkedin.com/in/libinprasanth/). Connect on LinkedIn for updates and discussion.
