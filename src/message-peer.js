// One peer of wurl --messages, in its own Node process, as a real peer runs on
// its own machine. The parent (messages.js) tells it when to join and when the
// mesh has settled; it reports every event back.
//
// From the parent:  { type: 'go', ids }   the mesh has settled: send, on this peer's own schedule
//                   { type: 'round', round, ids, spreadMs }   --soak: send one broadcast and one
//                                          direct message now, each after a random 0..spreadMs
//                   { type: 'final' }      report links, then stop
// To the parent:    { type: 'event', kind: 'joined' | 'link-up' | 'link-down' | 'sent' | 'received' | 'error', ... }
//                   { type: 'done' }       this peer has sent everything it was going to
//                   { type: 'final', links, werift }
import { containWeriftErrors, installWebRTC, weriftErrors, webrtcEngine } from './webrtc.js';

const config = JSON.parse(process.env.WURL_PEER);
const { i, target, joinDelayMs, sendDelaysMs, targetIndex, signalUrl, run, engine } = config;
const now = () => performance.timeOrigin + performance.now(); // comparable across processes
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
// Once wurl itself has gone (Ctrl-C reaches it first), the channel is closed:
// say nothing more. A send on a closed channel fails later as an 'error' event
// (EPIPE), not a throw, so the callback is what keeps it from flooding the
// terminal; the 'disconnect' handler below then leaves the room.
const report = (message) => {
  if (!process.connected) return;
  try { process.send?.(message, () => {}); } catch {}
};
process.on('error', () => {});
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
// WURL_DIAG=<dir>: each peer writes what it saw to <dir>/<n>.json when the run
// ends (its links, its ring neighbours, why links went down, the relay), so a
// mesh that did not form can be read afterwards.
const DIAG_DIR = process.env.WURL_DIAG || '';
const linkDowns = [];
if (DIAG_DIR) {
  const stamp = () => Math.round(now());
  const disconnect = node.mesh.disconnectFromPeer.bind(node.mesh);
  node.mesh.disconnectFromPeer = (peerId, reason) => {
    linkDowns.push({ at: stamp(), peer: String(peerId), why: `local:${reason}` });
    return disconnect(peerId, reason);
  };
  const watch = setInterval(() => {
    const client = node.mesh.signalingClient;
    if (!client || client.__wurlDiag) return;
    client.__wurlDiag = true;
    client.on('rtc:disconnected', (data) => linkDowns.push({ at: stamp(), peer: String(data?.peerId), why: `remote:${data?.reason}` }));
  }, 200);
  watch.unref?.();
}
// The last relay/signaling lines, for the diagnostic file.
const signalLog = [];
node.mesh?.on?.('signaling:log', (event) => {
  const text = String(event?.message ?? '');
  if (!/\[signal\]|\[relay\]/.test(text)) return;
  signalLog.push(`${new Date().toISOString().slice(11, 19)} ${text.slice(0, 200)}`);
  if (signalLog.length > 60) signalLog.shift();
});
const writeDiag = async (links) => {
  if (!DIAG_DIR) return;
  try {
    const m = node.mesh;
    const fs = await import('node:fs');
    const path = await import('node:path');
    fs.mkdirSync(DIAG_DIR, { recursive: true });
    const short = (id) => String(id).slice(0, 8);
    const connected = new Set(m.getConnectedPeers());
    const coordinate = [...(m.cecrCoordinateNeighbors?.() ?? [])];
    fs.writeFileSync(path.join(DIAG_DIR, `${i}.json`), JSON.stringify({
      i, id: m.getClientId(), links,
      coordinate: coordinate.map((peerId) => ({ peer: peerId, linked: connected.has(peerId), relayListed: m.activeSignalingPeers?.has(peerId) ?? null, connecting: m.connecting?.has(peerId) ?? null, backedOff: m.isPeerBackedOff?.(peerId) ?? null })),
      membership: m.getGlobalPeers().length,
      relayListed: m.activeSignalingPeers?.size ?? null,
      foreignListed: [...(m.activeSignalingPeers ?? [])].filter((peerId) => !m.globalPeers?.has(peerId)).map(short),
      pending: m.getPendingPeerCount?.() ?? null,
      relay: { connected: m.signalingClient?.isConnected?.() ?? null, idle: m.signalingClient?.client?.relayIdle ?? null, registered: m.signalingClient?.client?.isRegistered ?? null, url: m.signalingClient?.currentSignalUrl ?? null },
      linkDowns,
      signalLog,
    }));
  } catch (error) {
    console.error('wurl diag:', error?.message ?? error);
  }
};
// The links PeerPigeon knows about between other peers, from its map of the
// network (each peer's own topology report). wurl only hears the links its
// own peers report, so when the mesh runs through peers that are not wurl's
// (another wurl on another machine), the links among those peers come from
// here. Without them one ring across two machines counted as a dozen groups.
const graphTimer = setInterval(() => {
  if (!node.mesh.getClientId?.()) return;
  try {
    const edges = node.mesh.getGraphSnapshot().edges.map((edge) => [String(edge.source), String(edge.target)]);
    emit({ kind: 'graph', edges, membership: node.mesh.getGlobalPeers().length });
  } catch {
    // best-effort
  }
}, 5000);
graphTimer.unref?.();
node.mesh.on('peer:connected', (peerId) => emit({ kind: 'link-up', peer: String(peerId) }));
// Why each dial happened, so link churn can be traced to what started it.
// PeerPigeon logs its bridge and rebalance dials just before making them;
// anything else is classified by where it came from and how many links this
// peer held at that moment (a dial at maxPeers or more is one that will end
// in a shed link).
{
  let pendingTag = null;
  node.mesh.on('signaling:log', (event) => {
    const text = String(event?.message ?? '');
    if (text.startsWith('[bridge]')) pendingTag = 'bridge';
    else if (text.startsWith('[rebalance]')) pendingTag = 'rebalance';
  });
  const dial = node.mesh.connectToPeerInternal?.bind(node.mesh);
  if (dial) {
    node.mesh.connectToPeerInternal = (peerId, allowTemporaryOverflow) => {
      const links = node.mesh.getConnectedPeerCount();
      const max = node.mesh.config?.maxPeers ?? Infinity;
      const min = node.mesh.config?.minPeers ?? 0;
      const already = node.mesh.peers?.has?.(peerId) || node.mesh.connecting?.has?.(peerId);
      let why = pendingTag;
      pendingTag = null;
      if (!why) {
        const stack = String(new Error().stack ?? '');
        why = stack.includes('maybeRebalanceForCloserPeer') ? 'rebalance'
          : links < min ? 'below-min'
            : links < max ? 'fill'
              : stack.includes('maintainPeerConnections') ? 'at-max' : 'other';
      }
      const result = dial(peerId, allowTemporaryOverflow);
      const started = !already && (node.mesh.peers?.has?.(peerId) || node.mesh.connecting?.has?.(peerId));
      if (started) {
        const m = node.mesh;
        emit({
          kind: 'dial', why: links >= max ? `${why}@max` : why, to: String(peerId),
          member: m.globalPeers?.has?.(peerId) ?? null, listed: m.activeSignalingPeers?.has?.(peerId) ?? null,
          membership: m.globalPeers?.size ?? null,
        });
      }
      return result;
    };
  }
}
// Why each link went down: closed by this peer (PeerPigeon's reason, e.g.
// capacity_shed), or by the transport or the other end (remote:<reason>).
const downReason = new Map();
{
  const disconnect = node.mesh.disconnectFromPeer.bind(node.mesh);
  node.mesh.disconnectFromPeer = (peerId, reason = 'local_close') => {
    downReason.set(String(peerId), { why: String(reason), at: now() });
    return disconnect(peerId, reason);
  };
  const watch = setInterval(() => {
    const client = node.mesh.signalingClient;
    if (!client || client.__wurlWhy) return;
    client.__wurlWhy = true;
    client.on('rtc:disconnected', (data) => {
      const peer = String(data?.peerId);
      const known = downReason.get(peer);
      if (!known || now() - known.at > 2000) downReason.set(peer, { why: `remote:${data?.reason ?? 'closed'}`, at: now() });
    });
  }, 200);
  watch.unref?.();
}
node.mesh.on('peer:disconnected', (peerId) => {
  const peer = String(peerId);
  const known = downReason.get(peer);
  const why = known && now() - known.at < 5000 ? known.why : 'unknown';
  downReason.delete(peer);
  emit({ kind: 'link-down', peer, why });
});

let go = null;
const settled = new Promise((resolve) => { go = resolve; });
let finishing = false;
// --soak: a round of one broadcast and one direct message, asked for by the
// parent every --round seconds. The direct target moves round to round, so
// over a long run every peer sends to many others.
let joinedId = null;
const soakRound = async ({ round, ids, spreadMs = 0 }) => {
  if (!joinedId || finishing || !Array.isArray(ids) || ids.length < 2) return;
  await sleep(Math.random() * spreadMs);
  if (finishing) return;
  const bToken = `b:${run}:r${round}:${i}`;
  const bId = node.gossip.broadcast({ __wurl: bToken }, { sender: joinedId, timestamp: Date.now() });
  emit({ kind: 'sent', token: bToken, via: 'broadcast', ok: Boolean(bId), round });
  await sleep(Math.random() * spreadMs);
  if (finishing) return;
  const n = ids.length;
  const toIndex = (i + 1 + (round % (n - 1))) % n;
  const to = ids[toIndex];
  const dToken = `d:${run}:r${round}:${i}:${toIndex}`;
  const neighbour = Boolean(to) && linksOf().includes(to);
  const dId = to ? node.gossip.sendDirect(to, { __wurl: dToken }) : null;
  emit({ kind: 'sent', token: dToken, via: 'direct', to: toIndex, ok: Boolean(dId), neighbour, round });
};
process.on('message', async (message) => {
  if (message?.type === 'go') go(message.ids);
  if (message?.type === 'round') soakRound(message).catch((error) => emit({ kind: 'error', message: String(error?.message ?? error) }));
  if (message?.type === 'final' && !finishing) {
    finishing = true;
    const links = linksOf();
    await writeDiag(links);
    await node.destroy().catch(() => {});
    report({ type: 'final', links, werift: { count: weriftErrors.count, last: weriftErrors.last } });
    setTimeout(() => process.exit(0), 50);
  }
});
// Ctrl-C reaches every peer process at once, and a parent that dies leaves
// its peers disconnected. Either way, say goodbye before exiting (destroy()
// tells our neighbours we are leaving and waits for that to go out), so the
// network drops us at once instead of waiting out our membership lease.
const leave = async () => {
  if (finishing) return;
  finishing = true;
  const quit = setTimeout(() => process.exit(0), 2000);
  quit.unref?.();
  // Write this peer's diagnostics on the way out too (WURL_DIAG): a run that
  // is stopped is usually the one worth looking at.
  await writeDiag(linksOf()).catch(() => {});
  await node.destroy().catch(() => {});
  process.exit(0);
};
process.on('SIGINT', leave);
process.on('SIGTERM', leave);
process.on('disconnect', leave);

try {
  await sleep(joinDelayMs);
  await node.start();
  const id = String(node.mesh.getClientId() || '');
  emit({ kind: 'joined', id, engine: webrtcEngine });
  joinedId = id;

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
