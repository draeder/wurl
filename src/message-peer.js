// One peer of wurl --messages, in its own Node process, as a real peer runs on
// its own machine. The parent (messages.js) tells it when to join and when the
// mesh has settled; it reports every event back.
//
// From the parent:  { type: 'go', ids }   the mesh has settled: send, on this peer's own schedule
//                   { type: 'final' }      report links, then stop
// To the parent:    { type: 'event', kind: 'joined' | 'link-up' | 'link-down' | 'sent' | 'received' | 'error', ... }
//                   { type: 'done' }       this peer has sent everything it was going to
//                   { type: 'final', links, werift }
import { containWeriftErrors, installWebRTC, weriftErrors, webrtcEngine } from './webrtc.js';

const config = JSON.parse(process.env.WURL_PEER);
const { i, target, joinDelayMs, sendDelaysMs, targetIndex, signalUrl, run, engine } = config;
const now = () => performance.timeOrigin + performance.now(); // comparable across processes
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
const report = (message) => { try { process.send?.(message); } catch {} };
const emit = (event) => report({ type: 'event', at: now(), i, ...event });

containWeriftErrors();
await installWebRTC({ engine });
const { PeerPigeonNode } = await import('./vendor/peerpigeon.js');

const { minPeers, maxPeers, tolerantPeers } = target.topology;
const node = new PeerPigeonNode({
  crypto: false, networkId: target.network, sessionId: target.room, signalingServer: signalUrl,
  minPeers, maxPeers, tolerantPeers,
});
const linksOf = () => (node.mesh.getConnectedPeers() || []).map(String);
const seen = new Set();
const take = (data, via) => {
  const token = data && typeof data === 'object' ? data.__wurl : null;
  if (typeof token === 'string' && !seen.has(token)) {
    seen.add(token);
    emit({ kind: 'received', token, via });
  }
};
node.gossip.on('messageReceived', ({ message, local }) => { if (!local) take(message?.data, 'broadcast'); });
node.gossip.on('directMessageReceived', ({ message }) => take(message?.data, 'direct'));
// Trace every hop of a test direct message: each frame as it leaves this
// peer (to whom) and as it arrives (from whom), so a lost one shows where it died.
const tokenOf = (raw) => {
  if (typeof raw !== 'string' || !raw.includes('"type":"direct"') || !raw.includes('"__wurl":"d:')) return null;
  try {
    const frame = JSON.parse(raw);
    return frame?.type === 'direct' && typeof frame.data?.__wurl === 'string' ? frame.data.__wurl : null;
  } catch {
    return null;
  }
};
const meshSend = node.mesh.send.bind(node.mesh);
node.mesh.send = (peerId, data) => {
  const token = tokenOf(data);
  try {
    const result = meshSend(peerId, data);
    if (token) emit({ kind: 'hop-out', token, to: String(peerId), ok: true });
    return result;
  } catch (error) {
    if (token) emit({ kind: 'hop-out', token, to: String(peerId), ok: false, error: String(error?.message ?? error) });
    throw error;
  }
};
node.mesh.on('peer:data', ({ peerId, data }) => {
  const token = tokenOf(typeof data === 'string' ? data : (data?.toString?.() ?? ''));
  if (token) emit({ kind: 'hop-in', token, from: String(peerId) });
});
node.mesh.on('peer:connected', (peerId) => emit({ kind: 'link-up', peer: String(peerId) }));
node.mesh.on('peer:disconnected', (peerId) => emit({ kind: 'link-down', peer: String(peerId) }));

let go = null;
const settled = new Promise((resolve) => { go = resolve; });
let finishing = false;
process.on('message', async (message) => {
  if (message?.type === 'go') go(message.ids);
  if (message?.type === 'final' && !finishing) {
    finishing = true;
    const links = linksOf();
    await node.destroy().catch(() => {});
    report({ type: 'final', links, werift: { count: weriftErrors.count, last: weriftErrors.last } });
    setTimeout(() => process.exit(0), 50);
  }
});
process.on('disconnect', () => process.exit(0));

try {
  await sleep(joinDelayMs);
  await node.start();
  const id = String(node.mesh.getClientId() || '');
  emit({ kind: 'joined', id, engine: webrtcEngine });

  // Nobody sends until the parent has seen the mesh form and settle.
  const ids = await settled;
  await sleep(sendDelaysMs[0]);
  const bToken = `b:${run}:${i}`;
  const bId = node.gossip.broadcast({ __wurl: bToken }, { sender: id, timestamp: Date.now() });
  emit({ kind: 'sent', token: bToken, via: 'broadcast', ok: Boolean(bId) });

  await sleep(sendDelaysMs[1]);
  const to = ids[targetIndex];
  const dToken = `d:${run}:${i}:${targetIndex}`;
  const neighbour = Boolean(to) && linksOf().includes(to);
  const dId = to ? node.gossip.sendDirect(to, { __wurl: dToken }) : null;
  emit({ kind: 'sent', token: dToken, via: 'direct', to: targetIndex, ok: Boolean(dId), neighbour });
} catch (error) {
  emit({ kind: 'error', message: String(error?.message ?? error) });
}
report({ type: 'done' });
