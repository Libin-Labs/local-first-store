import {
  openLocalFirstStore,
  LocalStoreError,
  type LocalPersistenceAdapter,
  type LocalSnapshot,
  type LocalFirstStore,
  type LocalRecord,
  type Json,
} from '@lp-labs/local-first-store';
const adapter: LocalPersistenceAdapter = { load: async () => null, save: async () => true };
const promise: Promise<LocalFirstStore> = openLocalFirstStore(adapter);
async function use() {
  const store = await promise;
  const record: LocalRecord = await store.set('x', { n: 1 });
  const snapshot: LocalSnapshot = store.snapshot();
  const value: Json = store.query((records) => records.map((row) => row.value));
  const stop = store.subscribe(
    (records) => records.length,
    (count) => {
      const numeric: number = count;
      void numeric;
    },
  );
  await store.markSynced('x', record.localRevision, 'r');
  await store.markConflict('x', record.localRevision, 'r');
  void snapshot;
  void value;
  stop();
  await store.reload();
  store.close();
  // @ts-expect-error query output must be JSON
  store.query(() => undefined);
}
void use;
void new LocalStoreError('state', 'Reload required.');
// @ts-expect-error CAS save must return boolean
openLocalFirstStore({ load: async () => null, save: async () => 'yes' });
