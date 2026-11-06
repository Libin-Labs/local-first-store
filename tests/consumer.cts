import {
  openLocalFirstStore,
  type LocalPersistenceAdapter,
  type LocalStoreOptions,
} from '@lp-labs/local-first-store';
const adapter: LocalPersistenceAdapter = { load: async () => null, save: async () => true };
const options: LocalStoreOptions = {
  schemaVersion: 2,
  migrations: { 1: (s) => ({ ...s, schemaVersion: 2 }) },
};
void openLocalFirstStore(adapter, options);
// @ts-expect-error bad subscription limit
openLocalFirstStore(adapter, { maxSubscribers: '2' });
