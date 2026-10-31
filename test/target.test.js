import assert from 'node:assert/strict';
import test from 'node:test';
import { resolveTarget } from '../src/target.js';

test('a site URL names its relay, network and room', () => {
  const { topology: _topology, ...target } = resolveTarget('https://decentralize.ooo/?network=network%3Atest&room=room-1');
  assert.deepEqual(
    target,
    { auto: true, site: 'https://decentralize.ooo/?network=network%3Atest&room=room-1', signalUrl: 'wss://decentralize.ooo/ws', signalUrls: ['wss://decentralize.ooo/ws'], network: 'network:test', room: 'room-1' },
  );
  const page = resolveTarget('https://peer.ooo/?networkName=n1&sessionId=s1');
  assert.equal(page.network, 'n1');
  assert.equal(page.room, 's1');
  assert.equal(resolveTarget('http://127.0.0.1:8788/').signalUrl, 'ws://127.0.0.1:8788/ws');
  assert.equal(resolveTarget('decentralize.ooo').signalUrl, 'wss://decentralize.ooo/ws');
});

test('a tunnel address keeps its /t/<node id> prefix', () => {
  const id = 'a'.repeat(64);
  assert.equal(resolveTarget(`https://peer.ooo/t/${id}/?room=r`).signalUrl, `wss://peer.ooo/t/${id}/ws`);
});

test('a relay URL, an explicit relay, and overrides', () => {
  assert.equal(resolveTarget('wss://decentralize.ooo/ws').signalUrl, 'wss://decentralize.ooo/ws');
  assert.equal(resolveTarget('https://x.example/?signalingServer=wss://peer.ooo/ws').signalUrl, 'wss://peer.ooo/ws');
  assert.equal(resolveTarget('https://x.example/?signalingServer=wss://peer.ooo/ws').auto, false, 'a named relay is used as named');
  assert.equal(resolveTarget('https://x.example/', { signal: 'ws://127.0.0.1:8788/ws' }).auto, false);
  const t = resolveTarget('https://x.example/?network=a&room=b', { network: 'c', room: 'd', signal: 'ws://127.0.0.1:1/ws' });
  assert.deepEqual([t.network, t.room, t.signalUrl], ['c', 'd', 'ws://127.0.0.1:1/ws']);
  assert.deepEqual([resolveTarget('https://x.example/').network, resolveTarget('https://x.example/').room], ['network:test', 'room:test']);
});

test('the topology comes from the URL the way PeerPigeon\'s page reads it', () => {
  const topo = (url, flags) => resolveTarget(url, flags).topology;
  assert.deepEqual(topo('https://x/?room=r'), { name: 'token-ring', minPeers: 1, maxPeers: 2, tolerantPeers: 1, source: 'default topology' });
  assert.equal(topo('https://x/?topology=partial-mesh').maxPeers, 5);
  assert.deepEqual(topo('https://x/?topology=dense-mesh&maxPeers=6'), { name: 'custom', minPeers: 2, maxPeers: 6, tolerantPeers: 1, source: 'URL' });
  assert.deepEqual(topo('https://x/?minPeers=9&maxPeers=4&tolerant=3'), { name: 'custom', minPeers: 4, maxPeers: 4, tolerantPeers: 3, source: 'URL' });
  assert.equal(topo('https://x/?topology=partial-mesh', { maxPeers: '7' }).maxPeers, 7);
});

test('--signal a,b spreads peers over several relays', () => {
  const t = resolveTarget('https://decentralize.ooo/?room=r', { signal: 'ws://cloud1.test:1/ws, ws://127.0.0.1:2/ws' });
  assert.equal(t.signalUrl, 'ws://cloud1.test:1/ws');
  assert.deepEqual(t.signalUrls, ['ws://cloud1.test:1/ws', 'ws://127.0.0.1:2/ws']);
});
