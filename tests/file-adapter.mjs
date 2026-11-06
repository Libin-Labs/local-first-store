import { mkdir, open, readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { replaceFile } from './replace-file.mjs';
const gates = new Map();
// Real file state with process-local CAS locking. No cross-process or power-loss guarantee.
export async function fileAdapter(name, seed = null) {
  const directory = new URL('./results/', import.meta.url);
  await mkdir(directory, { recursive: true });
  const path = fileURLToPath(new URL(`${name}.json`, directory));
  await writeFile(path, JSON.stringify(seed));
  const reopen = () => ({
    async load() {
      await (gates.get(path) ?? Promise.resolve());
      return JSON.parse(await readFile(path, 'utf8'));
    },
    async save(snapshot, expectedRevision) {
      const captured = structuredClone(snapshot);
      const previous = gates.get(path) ?? Promise.resolve();
      const operation = previous.then(async () => {
        const state = JSON.parse(await readFile(path, 'utf8'));
        if ((state?.revision ?? null) !== expectedRevision) return false;
        const handle = await open(`${path}.next`, 'w');
        try {
          await handle.writeFile(JSON.stringify(captured));
          await handle.sync();
        } finally {
          await handle.close();
        }
        await replaceFile(`${path}.next`, path);
        return true;
      });
      gates.set(
        path,
        operation.catch(() => undefined),
      );
      return operation;
    },
  });
  return { adapter: reopen(), reopen, path };
}
