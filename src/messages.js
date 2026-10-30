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
const GRAPH_FRESH_MS = 12_000;

// Peers that are not wurl's (a browser tab in the same room) are part of the
// mesh too: a ring can pass through them. They join groups and count toward
// links, but group sizes count wurl's peers only. Leaving them out split one
// ring into as many "groups" as there were outside peers in it, and the mesh
// never counted as formed.
//
// The links between two outside peers come from PeerPigeon's own map of the
// network (outsideLinks, [a, b] pairs): wurl's peers do not see them. Without
// them a ring that ran through another wurl's peers on another machine counted
// as one group per stretch of ours.
function meshShape(ids, linksOf, outsideLinks = []) {
  const known = new Set(ids);
  const adjacency = new Map(ids.map((id) => [id, new Set()]));
  const outside = new Set();
  for (const id of ids) {
    for (const other of linksOf(id)) {
      if (!other || other === id) continue;
      if (!known.has(other)) {
        outside.add(other);
        if (!adjacency.has(other)) adjacency.set(other, new Set());
      }
      adjacency.get(id).add(other);
      adjacency.get(other).add(id);
    }
  }
  for (const [left, right] of outsideLinks) {
    if (!left || !right || left === right || known.has(left) || known.has(right)) continue;
    for (const peerId of [left, right]) {
      outside.add(peerId);
      if (!adjacency.has(peerId)) adjacency.set(peerId, new Set());
    }
    adjacency.get(left).add(right);
    adjacency.get(right).add(left);
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
      if (known.has(next)) size += 1;
      for (const other of adjacency.get(next)) {
        if (!seen.has(other)) { seen.add(other); queue.push(other); }
      }
    }
    groups.push(size);
  }
  groups.sort((a, b) => b - a);
  const degrees = ids.map((id) => adjacency.get(id).size);
  return { groups, degrees, adjacency, outside: outside.size };
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
    mesh: { formedAt, groups: shape.groups, degrees: shape.degrees, outside: shape.outside, own, minLinks, maxLinks, tolerantPeers },
    broadcast,
    direct,
    seconds: elapsed(),
  };
}

const jitter = (maxMs) => Math.floor(Math.random() * Math.max(0, maxMs));
const wallClock = () => performance.timeOrigin + performance.now(); // comparable across processes

/**
 * The asynchronous test, one Node process per peer (message-peer.js), as real
 * peers each run on their own machine. Each peer joins at its own random time
 * within the join window. This process only watches: it builds the mesh from
 * what the peers report, and once the mesh is formed (every peer joined, one
 * group, each with minPeers links) and has held for settleS it tells every
 * peer to go. Each then sends its broadcast and its direct message after its
 * own random delays. Deliveries are recorded as the peers report them; each
 * latency is measured from that message's own send time.
 */
async function runAsyncMessageTest({ target, peers: count, timeS = 120, waitS = 20, joinWindowS = 20, sendSpreadS = 10, settleS = 3, webrtc = 'auto', onProgress = () => {} }) {
  const { fork } = await import('node:child_process');
  const { minPeers: minLinks, maxPeers: maxLinks, tolerantPeers } = target.topology;
  const started = wallClock();
  const elapsed = () => (wallClock() - started) / 1000;
  const deadline = started + timeS * 1000;
  const urls = target.signalUrls?.length ? target.signalUrls : [target.signalUrl];
  const run = Date.now().toString(36);

  const ids = new Array(count).fill('');
  const indexOf = new Map();
  const joinedAt = new Array(count).fill(null);
  const sides = Array.from({ length: count }, () => new Set()); // links each peer itself reports
  const sent = new Map();       // token -> { token, from, via, sentAt, to, ... }
  const deliveries = new Map(); // token -> Map(peer index -> at)
  const finals = new Map();     // peer index -> { links, werift }
  const engines = new Map();
  const hops = new Map();       // direct token -> [{ kind: hop-out | hop-in, at, peer, other, ok }]
  const exited = new Set();
  const graphs = new Map();     // peer index -> { at, edges }: PeerPigeon's map of the network
  // Links between peers that are not wurl's, as wurl's peers' maps show them
  // now (a map older than GRAPH_FRESH_MS is left out).
  const outsideLinks = () => {
    const cutoff = wallClock() - GRAPH_FRESH_MS;
    const pairs = [];
    for (const { at, edges } of graphs.values()) if (at >= cutoff) pairs.push(...edges);
    return pairs;
  };
  let doneCount = 0;
  let ending = false;
  let formedAt = null;
  let steadySince = null;
  let settledAt = null;
  let allJoinedAt = null;
  let allLinkedAt = null;
  let lastProgress = -1;

  const children = Array.from({ length: count }, (_, i) => {
    const child = fork(new URL('./message-peer.js', import.meta.url), [], {
      env: {
        ...process.env,
        WURL_PEER: JSON.stringify({
          i,
          target: { network: target.network, room: target.room, topology: target.topology },
          joinDelayMs: jitter(joinWindowS * 1000),
          sendDelaysMs: [jitter(sendSpreadS * 1000), jitter(sendSpreadS * 1000)],
          targetIndex: (i + Math.floor(count / 2)) % count,
          signalUrl: urls[i % urls.length],
          run,
          engine: webrtc,
        }),
      },
      stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
    });
    child.on('message', (message) => {
      if (message?.type === 'done') { doneCount += 1; return; }
      if (message?.type === 'final') { finals.set(i, message); return; }
      if (message?.type !== 'event') return;
      const { at, kind } = message;
      if (kind === 'joined') {
        ids[i] = message.id;
        indexOf.set(message.id, i);
        joinedAt[i] = at;
        if (message.engine) engines.set(message.engine, (engines.get(message.engine) ?? 0) + 1);
      } else if (kind === 'graph') {
        if (!ending) graphs.set(i, { at: wallClock(), edges: Array.isArray(message.edges) ? message.edges : [] });
      } else if (kind === 'link-up') {
        if (!ending) sides[i].add(message.peer);
      } else if (kind === 'link-down') {
        if (!ending) sides[i].delete(message.peer); // shutting down drops every link: not news
      } else if (kind === 'sent') {
        sent.set(message.token, {
          token: message.token, from: i, sent: message.ok, sentAt: at, beforeMesh: settledAt === null,
          to: message.via === 'direct' ? new Set([message.to]) : new Set(children.map((_, j) => j).filter((j) => j !== i)),
          via: message.via, toIndex: message.to, neighbour: Boolean(message.neighbour),
        });
      } else if (kind === 'received') {
        if (!deliveries.has(message.token)) deliveries.set(message.token, new Map());
        if (!deliveries.get(message.token).has(i)) deliveries.get(message.token).set(i, at);
      } else if (kind === 'hop-out' || kind === 'hop-in') {
        if (!hops.has(message.token)) hops.set(message.token, []);
        const other = message.to ?? message.from;
        // The link as both ends report it at the moment the frame left.
        const j = indexOf.get(other);
        const link = kind !== 'hop-out' ? null
          : sides[i].has(other) && j !== undefined && sides[j].has(ids[i]) ? 'up at both ends'
            : sides[i].has(other) ? 'up only at the sender' : j !== undefined && sides[j].has(ids[i]) ? 'up only at the receiver' : 'down';
        hops.get(message.token).push({ kind, at, peer: i, other, ok: message.ok !== false, error: message.error, link });
      } else if (kind === 'error') {
        onProgress(`peer ${i} error: ${message.message}`);
      }
    });
    child.on('exit', (code) => {
      exited.add(i);
      if (!finals.has(i) && code !== 0) onProgress(`peer ${i} process exited with code ${code}`);
    });
    return child;
  });

  // The mesh as the peers report it (a link counts if either end lists it).
  const shapeNow = () => {
    const live = ids.filter(Boolean);
    return meshShape(live, (id) => [...sides[indexOf.get(id)]], outsideLinks());
  };
  const watchOnce = () => {
    const shape = shapeNow();
    const joined = joinedAt.filter((value) => value !== null).length;
    const own = sides.map((set) => set.size);
    if (allJoinedAt === null && joined === count) allJoinedAt = elapsed();
    if (allLinkedAt === null && joined === count && own.every((n) => n > 0)) allLinkedAt = elapsed();
    const formed = joined === count && shape.groups.length === 1 && Math.min(...shape.degrees) >= Math.min(minLinks, count - 1);
    if (formed && formedAt === null) formedAt = elapsed();
    if (formed) {
      steadySince ??= elapsed();
      if (settledAt === null && elapsed() - steadySince >= settleS) {
        settledAt = elapsed();
        for (const child of children) { try { child.send({ type: 'go', ids }); } catch {} }
      }
    } else if (settledAt === null) {
      steadySince = null;
    }
    if (Math.floor(elapsed()) !== lastProgress) {
      lastProgress = Math.floor(elapsed());
      const links = own.length ? own : [0];
      const broadcasts = [...sent.values()].filter((m) => m.via === 'broadcast').length;
      onProgress(`[${elapsed().toFixed(1)}s] joined ${joined}/${count}; links per peer min ${Math.min(...links)} avg ${(links.reduce((a, b) => a + b, 0) / links.length).toFixed(1)} max ${Math.max(...links)}; ${shape.groups.length <= 1 ? 'one group' : `${shape.groups.length} groups`}; sent ${broadcasts} broadcasts, ${sent.size - broadcasts} direct`);
    }
  };

  // Until every peer has sent (or the time is up), then until everything has
  // arrived or waitS after the last send.
  while (doneCount < count && wallClock() < deadline && exited.size < count) {
    watchOnce();
    await sleep(250);
  }
  const lastSend = Math.max(0, ...[...sent.values()].map((m) => m.sentAt));
  const complete = () => [...sent.values()].every((m) => [...m.to].every((j) => deliveries.get(m.token)?.has(j)));
  while (!complete() && wallClock() < lastSend + waitS * 1000) {
    watchOnce();
    await sleep(100);
  }

  ending = true;
  for (const child of children) { try { child.send({ type: 'final' }); } catch {} }
  const finalDeadline = wallClock() + 15_000;
  while (finals.size + [...exited].filter((i) => !finals.has(i)).length < count && wallClock() < finalDeadline) await sleep(50);
  for (const child of children) { try { child.kill(); } catch {} }

  const linksAtEnd = (i) => finals.get(i)?.links ?? [...sides[i]];
  const shape = meshShape(ids.filter(Boolean), (id) => linksAtEnd(indexOf.get(id)), outsideLinks());
  const own = ids.map((_, i) => linksAtEnd(i).length);
  const settle = (m) => {
    const got = deliveries.get(m.token) ?? new Map();
    const arrived = [...m.to].filter((j) => got.has(j));
    const lateReceivers = [...m.to].filter((j) => joinedAt[j] !== null && joinedAt[j] > m.sentAt);
    const result = { ...m, arrived, lateReceivers, lateArrived: lateReceivers.filter((j) => got.has(j)), latencies: arrived.map((j) => got.get(j) - m.sentAt) };
    if (m.via === 'direct' && arrived.length === 0) result.trace = traceOf(m);
    return result;
  };
  // Where a lost direct message went: every frame that left a peer, whether
  // the next peer received it, and whether that link was up at both ends.
  const traceOf = (m) => {
    const events = hops.get(m.token) ?? [];
    const outs = events.filter((e) => e.kind === 'hop-out');
    const ins = events.filter((e) => e.kind === 'hop-in');
    return outs.map((out) => {
      const to = indexOf.get(out.other);
      const arrived = to !== undefined && ins.some((e) => e.peer === to && e.other === ids[out.peer]);
      return {
        from: out.peer, to: to ?? out.other, at: out.at, sent: out.ok, error: out.error ?? null,
        arrived, link: out.link,
      };
    });
  };
  const all = [...sent.values()].sort((a, b) => a.from - b.from);
  const werift = [...finals.values()].reduce((sum, f) => ({ count: sum.count + (f.werift?.count ?? 0), last: f.werift?.last ?? sum.last }), { count: 0, last: null });
  return {
    ids,
    mode: 'async',
    processes: count,
    engines: Object.fromEntries(engines),
    joinWindowS,
    sendSpreadS,
    allJoinedAt,
    allLinkedAt,
    settledAt,
    settleS,
    mesh: { formedAt, groups: shape.groups, degrees: shape.degrees, outside: shape.outside, own, minLinks, maxLinks, tolerantPeers },
    broadcast: all.filter((m) => m.via === 'broadcast').map(settle),
    direct: all.filter((m) => m.via === 'direct').map(settle),
    werift,
    seconds: elapsed(),
  };
}

/** Asynchronous by default; { phased: true } runs the lockstep test. */
export async function runMessageTest(options) {
  return options.phased ? { ...(await runPhasedMessageTest(options)), mode: 'phased' } : runAsyncMessageTest(options);
}
