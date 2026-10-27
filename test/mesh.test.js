// End to end: a local freertc relay (the one in the freertc package) and wurl
// peers that must register, find each other and open data channels.
import assert from 'node:assert/strict';
import { spawn, execFile } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { describe, it } from 'node:test';
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
  // Before Node 22.13 the relay needs this flag for node:sqlite; given here, it
  // doesn't re-run itself in a child process.
  const [major, minor] = process.versions.node.split('.').map(Number);
  const flags = major === 22 && minor < 13 ? ['--experimental-sqlite'] : [];
  const relay = spawn(process.execPath, [...flags, '--no-warnings', RELAY], {
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

// P2P is parallel: each test has its own relay and port, so they all run at once.
describe('wurl against a local freertc relay', { concurrency: true }, () => {
it('three wurl peers form one mesh and answer each other\'s pings', async (t) => {
  const port = await startRelay(t);
  const { code, stdout } = await wurl(['--json', '-p', '3', '-t', '20', `http://127.0.0.1:${port}/?network=network:test&room=mesh-${Date.now()}`]);
  const report = JSON.parse(stdout);
  assert.equal(code, 0, stdout);
  for (const peer of report.peers) {
    assert.notEqual(peer.registered_s, null);
    assert.equal(peer.connected.length, 2, `${peer.id.slice(0, 8)} has ${peer.connected.length} links`);
    assert.ok(peer.connected.some((c) => c.rtt_ms !== null), 'a round trip was measured');
  }
});

it('an empty room exits 52', async (t) => {
  const port = await startRelay(t);
  assert.equal((await wurl(['-s', '-t', '10', `http://127.0.0.1:${port}/?room=empty-${Date.now()}`])).code, 52);
});

it('an unreachable relay exits 6', async () => {
  assert.equal((await wurl(['-s', '-t', '3', 'http://127.0.0.1:9/?room=nobody'])).code, 6);
});

it('--messages: peers on several threads join and send on their own schedules, and the mesh carries their messages', async (t) => {
  const port = await startRelay(t);
  const { stdout } = await wurl(['--messages', '--json', '-p', '6', '--threads', '2', '-t', '40', '--join-window', '4', '--send-spread', '2', `http://127.0.0.1:${port}/?room=tmsg-${Date.now()}&topology=dense-mesh`]);
  const report = JSON.parse(stdout);
  assert.equal(report.mode, 'async');
  assert.equal(report.threads, 2);
  assert.equal(report.peers, 6);
  assert.equal(report.mesh.groups.length, 1, 'one mesh at the end');
  assert.ok(report.broadcast.delivered > 0 && report.direct.delivered > 0, 'messages crossed the mesh');
});

// Every message arrives, late joiners included.
// Known gap, kept visible: a peer that joins after a broadcast was sent does not
// get it (PeerPigeon's anti-entropy repair does not reach late joiners), and a
// direct message to a peer that has only just joined can be lost.
it('--messages: asynchronous joins and sends, every message arrives', { todo: 'late joiners miss broadcasts sent before they joined (PeerPigeon anti-entropy)' }, async (t) => {
  const port = await startRelay(t);
  const { code, stdout } = await wurl(['--messages', '--json', '-p', '5', '-t', '40', '--join-window', '5', '--send-spread', '2', `http://127.0.0.1:${port}/?room=amsg-${Date.now()}&topology=dense-mesh`]);
  const report = JSON.parse(stdout);
  assert.equal(report.mode, 'async');
  assert.equal(report.broadcast.delivered, report.broadcast.expected);
  assert.equal(report.direct.delivered, report.direct.expected);
  assert.equal(code, 0, stdout);
});

it('ten wurl peers in one process form one mesh', async (t) => {
  const port = await startRelay(t);
  const { code, stdout } = await wurl(['--json', '-p', '10', '-t', '30', `http://127.0.0.1:${port}/?room=ten-${Date.now()}`]);
  assert.equal(code, 0, stdout);
  const report = JSON.parse(stdout);
  assert.ok(report.peers.every((peer) => peer.connected.length >= 1));
});
});
