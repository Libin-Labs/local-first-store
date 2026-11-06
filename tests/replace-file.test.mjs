import test from 'node:test';
import assert from 'node:assert/strict';
import { replaceFile } from './replace-file.mjs';

test('Windows replacement retries only failed renames and succeeds without deleting either path', async () => {
  const calls = [];
  const waits = [];
  await replaceFile('staging', 'snapshot', {
    platform: 'win32',
    renameOperation: async (...paths) => {
      calls.push(paths);
      if (calls.length <= 3)
        throw Object.assign(new Error('Transient lock'), {
          code: ['EPERM', 'EACCES', 'EBUSY'][calls.length - 1],
        });
    },
    wait: async (ms) => {
      waits.push(ms);
    },
  });
  assert.deepEqual(
    calls,
    Array.from({ length: 4 }, () => ['staging', 'snapshot']),
  );
  assert.deepEqual(waits, [25, 50, 100]);
});
test('persistent Windows locks stop at eight attempts and preserve the final original error', async () => {
  const errors = [];
  const waits = [];
  await assert.rejects(
    replaceFile('staging', 'snapshot', {
      platform: 'win32',
      renameOperation: async () => {
        const error = Object.assign(new Error(`Lock ${errors.length}`), { code: 'EPERM' });
        errors.push(error);
        throw error;
      },
      wait: async (ms) => {
        waits.push(ms);
      },
    }),
    (error) => error === errors[7],
  );
  assert.equal(errors.length, 8);
  assert.deepEqual(waits, [25, 50, 100, 200, 200, 200, 200]);
  assert.equal(
    waits.reduce((total, ms) => total + ms, 0),
    975,
  );
});
test('unrelated Windows errors and all non-Windows errors fail immediately with original identity', async () => {
  for (const [platform, code] of [
    ['win32', 'ENOENT'],
    ['win32', 'ENOSPC'],
    ['linux', 'EPERM'],
  ]) {
    const original = Object.assign(new Error(code), { code });
    let calls = 0;
    await assert.rejects(
      replaceFile('staging', 'snapshot', {
        platform,
        renameOperation: async () => {
          calls++;
          throw original;
        },
        wait: async () => {
          assert.fail('Must not wait');
        },
      }),
      (error) => error === original,
    );
    assert.equal(calls, 1);
  }
});
