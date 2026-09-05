// Copy non-TS assets (node icons) into dist/, mirroring the source layout.
const fs = require('fs');
const path = require('path');

const srcRoot = path.join(__dirname, '..', 'nodes');
const outRoot = path.join(__dirname, '..', 'dist', 'nodes');

function walk(dir, rel = '') {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const from = path.join(dir, entry.name);
    const relPath = path.join(rel, entry.name);
    if (entry.isDirectory()) {
      walk(from, relPath);
      continue;
    }
    if (/\.(svg|png)$/i.test(entry.name)) {
      const to = path.join(outRoot, relPath);
      fs.mkdirSync(path.dirname(to), { recursive: true });
      fs.copyFileSync(from, to);
      console.log('asset', relPath);
    }
  }
}

walk(srcRoot);
