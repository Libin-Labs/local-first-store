import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
for (const config of ['tsconfig.json', 'tsconfig.cjs.json'])
  execFileSync(process.execPath, ['node_modules/typescript/bin/tsc', '-p', config], {
    stdio: 'inherit',
  });
mkdirSync('dist/cjs', { recursive: true });
writeFileSync('dist/cjs/package.json', '{"type":"commonjs"}\n');
