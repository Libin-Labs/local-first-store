import { rename } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';

export async function replaceFile(
  source,
  destination,
  { renameOperation = rename, wait = delay, platform = process.platform } = {},
) {
  for (let attempt = 0; attempt < 8; attempt++) {
    try {
      await renameOperation(source, destination);
      return;
    } catch (error) {
      if (
        platform !== 'win32' ||
        !['EPERM', 'EACCES', 'EBUSY'].includes(error.code) ||
        attempt === 7
      )
        throw error;
      await wait(Math.min(25 * 2 ** attempt, 200));
    }
  }
}
