// wurl --messages: PeerPigeon peers in one room. Wait for one partial mesh,
// then every peer broadcasts once (gossip should carry it to every other peer)
// and sends one direct message to the peer halfway round, which is usually
// not a neighbour, so the mesh has to route it.
import { PeerPigeonNode } from './vendor/peerpigeon.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function meshShape(ids, linksOf) {
  const known = new Set(ids);
  const adjacency = new Map(ids.map((id) => [id, new Set()]));
  for (const id of ids) {
    for (const other of linksOf(id)) {
      if (!known.has(other)) continue;
      adjacency.get(id).add(other);
      adjacency.get(other).add(id);
    }
  }
  const groups = [];
  const seen = new Set();
  for (const id of ids) {
    if (seen.has(id)) continue;
    const queue = [id];
    seen.add(id);
    let size = 0;
    while (queue.length) {
      const next = queue.shift();
      size += 1;
      for (const other of adjacency.get(next)) {
        if (!seen.has(other)) { seen.add(other); queue.push(other); }
      }
    }
    groups.push(size);
  }
  groups.sort((a, b) => b - a);
  const degrees = ids.map((id) => adjacency.get(id).size);
  return { groups, degrees, adjacency };
}

/**
 * Run the test. Returns { ids, mesh, broadcast, direct } for the report.
 * onProgress(line) gets one line per second while the mesh forms.
 */
export async function runMessageTest({ target, peers: count, minLinks = 2, maxLinks = 10, timeS = 60, waitS = 20, settleS = 3, onProgress = () => {} }) {
  const started = performance.now();
  const elapsed = () => (performance.now() - started) / 1000;
  const nodes = [];
  const received = [];   // per node: Map token -> received at (ms, performance.now())

  for (let i = 0; i < count; i += 1) {
    const node = new PeerPigeonNode({
      crypto: false,
      networkId: target.network,
      sessionId: target.room,
      signalingServer: target.signalUrl,
      minPeers: minLinks,
      maxPeers: maxLinks,
    });
    const inbox = new Map();
    const take = (data) => {
      const token = data && typeof data === 'object' ? data.__wurl : null;
      if (typeof token === 'string' && !inbox.has(token)) inbox.set(token, performance.now());
    };
    node.gossip.on('messageReceived', ({ message, local }) => { if (!local) take(message?.data); });
    node.gossip.on('directMessageReceived', ({ message }) => take(message?.data));
    nodes.push(node);
    received.push(inbox);
  }
  await Promise.all(nodes.map((node) => node.start()));

  const idOf = (node) => String(node.mesh.getClientId() || '');
  const linksOf = (node) => (node.mesh.getConnectedPeers() || []).map(String);
  const snapshot = () => {
    const ids = nodes.map(idOf);
    const byId = new Map(nodes.map((node, i) => [ids[i], node]));
    return { ids, ...meshShape(ids.filter(Boolean), (id) => linksOf(byId.get(id))) };
  };

  // 1. The mesh: one group of every peer, each with at least minLinks, held
  //    steady for settleS (or the time runs out).
  let shape = snapshot();
  let formedAt = null;
  let steadySince = null;
  let lastProgress = 0;
  while (elapsed() < timeS) {
    shape = snapshot();
    const formed = shape.ids.every(Boolean) && shape.groups.length === 1 && Math.min(...shape.degrees) >= Math.min(minLinks, count - 1);
    if (formed) {
      formedAt ??= elapsed();
      steadySince ??= elapsed();
      if (elapsed() - steadySince >= settleS) break;
    } else {
      steadySince = null;
    }
    if (elapsed() - lastProgress >= 1) {
      lastProgress = elapsed();
      const links = shape.degrees.length ? shape.degrees : [0];
      onProgress(`[${elapsed().toFixed(1)}s] links per peer min ${Math.min(...links)} avg ${(links.reduce((a, b) => a + b, 0) / links.length).toFixed(1)} max ${Math.max(...links)}; ${shape.groups.length === 1 ? 'one group' : `${shape.groups.length} groups`}`);
    }
    await sleep(250);
  }
  const ids = shape.ids;
  const run = Date.now().toString(36);

  async function deliver(messages) {
    // messages: [{ token, from (index), to: Set of indexes, sentAt }]
    const deadline = performance.now() + waitS * 1000;
    while (performance.now() < deadline) {
      if (messages.every((m) => [...m.to].every((i) => received[i].has(m.token)))) break;
      await sleep(100);
    }
    return messages.map((m) => {
      const arrived = [...m.to].filter((i) => received[i].has(m.token));
      return { ...m, arrived, latencies: arrived.map((i) => received[i].get(m.token) - m.sentAt) };
    });
  }

  // 2. Broadcast: every peer once; every other peer should receive each.
  const broadcasts = nodes.map((node, i) => {
    const token = `b:${run}:${i}`;
    const sentAt = performance.now();
    const id = node.gossip.broadcast({ __wurl: token }, { sender: ids[i], timestamp: Date.now() });
    return { token, from: i, sent: Boolean(id), sentAt, to: new Set(nodes.map((_, j) => j).filter((j) => j !== i)) };
  });
  const broadcast = await deliver(broadcasts);

  // 3. Direct: each peer to the one halfway round; usually several hops away.
  const directs = nodes.map((node, i) => {
    const j = (i + Math.floor(count / 2)) % count;
    const token = `d:${run}:${i}:${j}`;
    const sentAt = performance.now();
    const id = node.gossip.sendDirect(ids[j], { __wurl: token });
    return { token, from: i, toIndex: j, sent: Boolean(id), sentAt, to: new Set([j]), neighbour: linksOf(node).includes(ids[j]) };
  });
  const direct = await deliver(directs);

  await Promise.all(nodes.map((node) => node.destroy().catch(() => {})));
  return {
    ids,
    mesh: { formedAt, groups: shape.groups, degrees: shape.degrees },
    broadcast,
    direct,
    seconds: elapsed(),
  };
}
