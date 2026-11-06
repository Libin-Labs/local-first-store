import { execFileSync } from 'node:child_process';
execFileSync(
  process.execPath,
  [
    'node_modules/typescript/bin/tsc',
    'tests/consumer.mts',
    'tests/consumer.cts',
    '--noEmit',
    '--strict',
    '--module',
    'NodeNext',
    '--moduleResolution',
    'NodeNext',
    '--target',
    'ES2022',
  ],
  { stdio: 'inherit' },
);
