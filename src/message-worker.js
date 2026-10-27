// One worker thread of wurl --messages: it runs its share of the peers, each on
// its own schedule, and reports every event to the main thread as it happens.
// Peers on different threads run at the same time, each thread with its own
// event loop, as peers on separate machines do.
//
// From the main thread:   { type: 'ids', ids }    peer ids known so far (for direct messages)
//                         { type: 'final' }       report each peer's links, then stop
// To the main thread:     { type: 'event', ... }  joined / link-up / link-down / sent / received
//                         { type: 'peer-done', i }
//                         { type: 'final', peers: [{ i, id, links }], werift }
import { parentPort, workerData } from 'node:worker_threads';
import { containWeriftErrors, installWebRTC, weriftErrors } from './webrtc.js';

await installWebRTC();
containWeriftErrors();
const { PeerPigeonNode } = await import('./vendor/peerpigeon.js');

const { target, plan, deadlineAt } = workerData; // plan: [{ i, joinDelayMs, sendDelaysMs: [b, d], targetIndex, signalUrl }]
const now = () => performance.timeOrigin + performance.now(); // comparable across threads
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
const emit = (event) => parentPort.postMessage({ type: 'event', at: now(), ...event });

const knownIds = new Map(); // peer index -> id
parentPort.on('message', (message) => {
  if (message?.type === 'ids') for (const [i, id] of message.ids) knownIds.set(i, id);
  if (message?.type === 'final') finish();
});

const nodes = new Map(); // peer index -> node
const linksOf = (node) => (node.mesh.getConnectedPeers() || []).map(String);

async function lifecycle({ i, joinDelayMs, sendDelaysMs, targetIndex, signalUrl }) {
  const { minPeers, maxPeers, tolerantPeers } = target.topology;
  const node = new PeerPigeonNode({
    crypto: false, networkId: target.network, sessionId: target.room, signalingServer: signalUrl,
    minPeers, maxPeers, tolerantPeers,
  });
  nodes.set(i, node);
  const seen = new Set();
  const take = (data, kind) => {
    const token = data && typeof data === 'object' ? data.__wurl : null;
    if (typeof token === 'string' && !seen.has(token)) {
      seen.add(token);
      emit({ kind: 'received', i, token, via: kind });
    }
  };
  node.gossip.on('messageReceived', ({ message, local }) => { if (!local) take(message?.data, 'broadcast'); });
  node.gossip.on('directMessageReceived', ({ message }) => take(message?.data, 'direct'));
  node.mesh.on('peer:connected', (peerId) => emit({ kind: 'link-up', i, peer: String(peerId) }));
  node.mesh.on('peer:disconnected', (peerId) => emit({ kind: 'link-down', i, peer: String(peerId) }));

  await sleep(joinDelayMs);
  await node.start();
  const id = String(node.mesh.getClientId() || '');
  emit({ kind: 'joined', i, id });

  // A peer talks once it has someone to talk to, not when the network is done.
  while (linksOf(node).length === 0 && now() < deadlineAt) await sleep(50);
  await sleep(sendDelaysMs[0]);
  const bToken = `b:${workerData.run}:${i}`;
  const bId = node.gossip.broadcast({ __wurl: bToken }, { sender: id, timestamp: Date.now() });
  emit({ kind: 'sent', i, token: bToken, via: 'broadcast', ok: Boolean(bId) });

  await sleep(sendDelaysMs[1]);
  // Only a peer that exists can be addressed: wait until the target has joined somewhere.
  while (!knownIds.has(targetIndex) && now() < deadlineAt) await sleep(50);
  const to = knownIds.get(targetIndex);
  const dToken = `d:${workerData.run}:${i}:${targetIndex}`;
  const neighbour = Boolean(to) && linksOf(node).includes(to);
  const dId = to ? node.gossip.sendDirect(to, { __wurl: dToken }) : null;
  emit({ kind: 'sent', i, token: dToken, via: 'direct', to: targetIndex, ok: Boolean(dId), neighbour });
  parentPort.postMessage({ type: 'peer-done', i });
}

let finishing = false;
async function finish() {
  if (finishing) return;
  finishing = true;
  const peers = [...nodes].map(([i, node]) => ({ i, id: String(node.mesh.getClientId() || ''), links: linksOf(node) }));
  await Promise.all([...nodes.values()].map((node) => node.destroy().catch(() => {})));
  parentPort.postMessage({ type: 'final', peers, werift: { count: weriftErrors.count, last: weriftErrors.last } });
}

await Promise.all(plan.map((peer) => lifecycle(peer).catch((error) => {
  emit({ kind: 'error', i: peer.i, message: String(error?.message ?? error) });
  parentPort.postMessage({ type: 'peer-done', i: peer.i });
})));
