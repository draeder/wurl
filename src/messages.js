// wurl --messages: PeerPigeon peers in one room. Every peer broadcasts once
// (gossip should carry it to every other peer) and sends one direct message to
// the peer halfway round, which is usually not a neighbour, so the mesh has to
// route it.
//
// By default the test is asynchronous, as a real P2P network is: each peer
// joins at its own random time within the join window, and sends its broadcast
// and its direct message at its own random times once it has a link, while
// others are still joining and links are still forming. Nothing waits for the
// mesh. --phased runs the old lockstep test instead: everyone joins at once,
// the mesh must form and hold, then all broadcasts, then all direct messages.
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
async function runPhasedMessageTest({ target, peers: count, timeS = 60, waitS = 20, settleS = 3, onProgress = () => {} }) {
  const { minPeers: minLinks, maxPeers: maxLinks, tolerantPeers } = target.topology;
  const started = performance.now();
  const elapsed = () => (performance.now() - started) / 1000;
  const nodes = [];
  const received = [];   // per node: Map token -> received at (ms, performance.now())

  for (let i = 0; i < count; i += 1) {
    const node = new PeerPigeonNode({
      crypto: false,
      networkId: target.network,
      sessionId: target.room,
      signalingServer: (target.signalUrls?.length ? target.signalUrls : [target.signalUrl])[i % (target.signalUrls?.length || 1)],
      minPeers: minLinks,
      maxPeers: maxLinks,
      tolerantPeers,
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
  // What each peer itself reports: a link counts once per end, so a peer over
  // maxPeers here is over its own limit, not double-counted.
  const own = nodes.map((node) => linksOf(node).length);
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
    mesh: { formedAt, groups: shape.groups, degrees: shape.degrees, own, minLinks, maxLinks, tolerantPeers },
    broadcast,
    direct,
    seconds: elapsed(),
  };
}

const jitter = (maxMs) => Math.floor(Math.random() * Math.max(0, maxMs));

/**
 * The asynchronous test. Every peer runs its own lifecycle concurrently:
 * join at a random time in [0, joinWindowS), wait for a first link, then after
 * its own random delays broadcast once and send one direct message. Deliveries
 * are recorded as they happen; each latency is measured from that message's
 * own send time.
 */
async function runAsyncMessageTest({ target, peers: count, timeS = 120, waitS = 20, joinWindowS = 20, sendSpreadS = 10, settleS = 3, onProgress = () => {} }) {
  const { minPeers: minLinks, maxPeers: maxLinks, tolerantPeers } = target.topology;
  const started = performance.now();
  const elapsed = () => (performance.now() - started) / 1000;
  const deadline = started + timeS * 1000;
  const urls = target.signalUrls?.length ? target.signalUrls : [target.signalUrl];
  const run = Date.now().toString(36);
  const nodes = [];
  const received = [];
  const joinedAt = new Array(count).fill(null);   // performance.now() when start() resolved
  const ids = new Array(count).fill('');
  const broadcasts = [];
  const directs = [];

  for (let i = 0; i < count; i += 1) {
    const node = new PeerPigeonNode({
      crypto: false,
      networkId: target.network,
      sessionId: target.room,
      signalingServer: urls[i % urls.length],
      minPeers: minLinks,
      maxPeers: maxLinks,
      tolerantPeers,
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

  const linksOf = (node) => (node.mesh.getConnectedPeers() || []).map(String);
  const snapshot = () => {
    const live = nodes.map((node, i) => (joinedAt[i] !== null ? String(node.mesh.getClientId() || '') : '')).filter(Boolean);
    const byId = new Map(nodes.map((node, i) => [ids[i], node]));
    return meshShape(live, (id) => linksOf(byId.get(id)));
  };

  // Watch the mesh. It is formed when every peer has joined, it is one group,
  // and every peer has at least minPeers links; it is settled once it has
  // stayed formed for settleS. Nobody sends before it is settled.
  let formedAt = null;
  let steadySince = null;
  let settledAt = null;
  let watching = true;
  let lastProgress = -1;
  const watcher = (async () => {
    while (watching) {
      const shape = snapshot();
      const joined = joinedAt.filter((at) => at !== null).length;
      const formed = joined === count && shape.groups.length === 1 && Math.min(...shape.degrees) >= Math.min(minLinks, count - 1);
      if (formed && formedAt === null) formedAt = elapsed();
      if (formed) {
        steadySince ??= elapsed();
        if (settledAt === null && elapsed() - steadySince >= settleS) settledAt = elapsed();
      } else if (settledAt === null) {
        steadySince = null;
      }
      if (Math.floor(elapsed()) !== lastProgress) {
        lastProgress = Math.floor(elapsed());
        const links = shape.degrees.length ? shape.degrees : [0];
        onProgress(`[${elapsed().toFixed(1)}s] joined ${joined}/${count}; links per peer min ${Math.min(...links)} avg ${(links.reduce((a, b) => a + b, 0) / links.length).toFixed(1)} max ${Math.max(...links)}; ${shape.groups.length <= 1 ? 'one group' : `${shape.groups.length} groups`}; sent ${broadcasts.length} broadcasts, ${directs.length} direct`);
      }
      await sleep(250);
    }
  })();

  let allJoinedAt = null;
  let allLinkedAt = null;
  const lifecycle = async (i) => {
    const node = nodes[i];
    await sleep(jitter(joinWindowS * 1000));
    await node.start();
    joinedAt[i] = performance.now();
    ids[i] = String(node.mesh.getClientId() || '');
    // No one sends until the mesh is formed and settled: every peer joined,
    // one group, each with minPeers links, held for settleS. After that each
    // peer sends on its own time.
    while (joinedAt.some((at) => at === null) && performance.now() < deadline) await sleep(100);
    if (allJoinedAt === null) allJoinedAt = (performance.now() - started) / 1000;
    while (nodes.some((peer) => linksOf(peer).length === 0) && performance.now() < deadline) await sleep(100);
    if (allLinkedAt === null) allLinkedAt = (performance.now() - started) / 1000;
    while (settledAt === null && performance.now() < deadline) await sleep(100);
    await sleep(jitter(sendSpreadS * 1000));
    const bToken = `b:${run}:${i}`;
    const bAt = performance.now();
    const bId = node.gossip.broadcast({ __wurl: bToken }, { sender: ids[i], timestamp: Date.now() });
    broadcasts.push({ token: bToken, from: i, sent: Boolean(bId), sentAt: bAt, beforeMesh: settledAt === null, to: new Set(nodes.map((_, j) => j).filter((j) => j !== i)) });
    await sleep(jitter(sendSpreadS * 1000));
    // Only a peer that exists can be addressed: wait for the target to have joined.
    const j = (i + Math.floor(count / 2)) % count;
    while (!ids[j] && performance.now() < deadline) await sleep(100);
    const dToken = `d:${run}:${i}:${j}`;
    const dAt = performance.now();
    const dId = ids[j] ? node.gossip.sendDirect(ids[j], { __wurl: dToken }) : null;
    directs.push({ token: dToken, from: i, toIndex: j, sent: Boolean(dId), sentAt: dAt, beforeMesh: settledAt === null, to: new Set([j]), neighbour: linksOf(node).includes(ids[j]) });
  };
  await Promise.all(nodes.map((_, i) => lifecycle(i)));

  // Wait for what is still in flight: until everything arrived, or waitS after the last send.
  const lastSend = Math.max(0, ...broadcasts.map((m) => m.sentAt), ...directs.map((m) => m.sentAt));
  const all = [...broadcasts, ...directs];
  while (performance.now() < lastSend + waitS * 1000) {
    if (all.every((m) => [...m.to].every((k) => received[k].has(m.token)))) break;
    await sleep(100);
  }
  const shape = snapshot();
  watching = false;
  await watcher;
  const own = nodes.map((node) => linksOf(node).length);
  const settle = (m) => {
    const arrived = [...m.to].filter((k) => received[k].has(m.token));
    // Receivers that joined after this was sent can only get it by anti-entropy repair.
    const lateReceivers = [...m.to].filter((k) => joinedAt[k] !== null && joinedAt[k] > m.sentAt);
    return { ...m, arrived, lateReceivers, lateArrived: lateReceivers.filter((k) => received[k].has(m.token)), latencies: arrived.map((k) => received[k].get(m.token) - m.sentAt) };
  };
  await Promise.all(nodes.map((node) => node.destroy().catch(() => {})));
  return {
    ids,
    mode: 'async',
    joinWindowS,
    sendSpreadS,
    allJoinedAt,
    allLinkedAt,
    settledAt,
    settleS,
    mesh: { formedAt, groups: shape.groups, degrees: shape.degrees, own, minLinks, maxLinks, tolerantPeers },
    broadcast: broadcasts.sort((a, b) => a.from - b.from).map(settle),
    direct: directs.sort((a, b) => a.from - b.from).map(settle),
    seconds: elapsed(),
  };
}

/** Asynchronous by default; { phased: true } runs the lockstep test. */
export async function runMessageTest(options) {
  return options.phased ? { ...(await runPhasedMessageTest(options)), mode: 'phased' } : runAsyncMessageTest(options);
}
