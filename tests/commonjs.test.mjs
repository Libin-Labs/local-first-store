import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import * as esm from '../dist/esm/index.js';
const cjs = createRequire(import.meta.url)('../dist/cjs/index.js');
test('CommonJS exports match ESM and execute optimistic save', async () => {
  assert.deepEqual(Object.keys(cjs).sort(), Object.keys(esm).sort());
  const store = await cjs.openLocalFirstStore({ load: async () => null, save: async () => true });
  await store.set('x', 1);
  assert.equal(store.get('x'), 1);
});
