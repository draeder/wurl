// End to end: a local freertc relay (the one in the freertc package) and wurl
// peers that must register, find each other and open data channels.
import assert from 'node:assert/strict';
import { spawn, execFile } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const require = createRequire(import.meta.url);
const RELAY = path.join(path.dirname(require.resolve('freertc/package.json')), 'scripts/node-relay.mjs');
const WURL = fileURLToPath(new URL('../bin/wurl.mjs', import.meta.url));
let nextPort = 19_000 + Math.floor(Math.random() * 400) * 2;

// Each test gets its own relay on its own port, and waits for it to exit, so
// one test's relay never holds the next one's port.
async function startRelay(t) {
  const port = nextPort;
  nextPort += 1;
  const relay = spawn(process.execPath, ['--no-warnings', RELAY], {
    env: { ...process.env, PORT: String(port), FREERTC_FEDERATION: 'off', FREERTC_DATA_DIR: mkdtempSync(path.join(tmpdir(), 'wurl-relay-')) },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  t.after(async () => {
    if (relay.exitCode !== null) return;
    const exited = new Promise((resolve) => relay.once('exit', resolve));
    relay.kill();
    await exited;
  });
  await new Promise((resolve, reject) => {
    relay.stdout.on('data', (chunk) => { if (/ on https?:\/\//.test(String(chunk))) resolve(); });
    relay.once('exit', (code) => reject(new Error(`relay exited with ${code}`)));
  });
  return port;
}

const wurl = (args) => promisify(execFile)(process.execPath, [WURL, ...args], { timeout: 60_000 })
  .then(({ stdout }) => ({ code: 0, stdout }), (error) => ({ code: error.code, stdout: error.stdout }));

test('three wurl peers form one mesh and answer each other\'s pings', async (t) => {
  const port = await startRelay(t);
  const { code, stdout } = await wurl(['--json', '-p', '3', '-t', '8', `http://127.0.0.1:${port}/?network=network:test&room=mesh-${Date.now()}`]);
  const report = JSON.parse(stdout);
  assert.equal(code, 0, stdout);
  for (const peer of report.peers) {
    assert.notEqual(peer.registered_s, null);
    assert.equal(peer.connected.length, 2, `${peer.id.slice(0, 8)} has ${peer.connected.length} links`);
    assert.ok(peer.connected.some((c) => c.rtt_ms !== null), 'a round trip was measured');
  }
});

test('an empty room exits 52 and an unreachable relay exits 6', async (t) => {
  const port = await startRelay(t);
  assert.equal((await wurl(['-s', '-t', '10', `http://127.0.0.1:${port}/?room=empty-${Date.now()}`])).code, 52);
  assert.equal((await wurl(['-s', '-t', '3', 'http://127.0.0.1:9/?room=nobody'])).code, 6);
});
