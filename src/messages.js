// wurl --messages: PeerPigeon peers in one room. Every peer broadcasts once
// (gossip should carry it to every other peer) and sends one direct message to
// the peer halfway round, which is usually not a neighbour, so the mesh has to
// route it.
//
// It runs the way a P2P network does. The peers are spread over worker threads
// (message-worker.js), each thread with its own event loop, so peers on
// different threads run at the same time. Each peer joins at its own random
// time within the join window, and once it has a link sends its broadcast and
// its direct message at its own random times, while others are still joining
// and links are still forming. Nothing waits for the mesh. Every event (join,
// link up or down, send, delivery) is reported the moment it happens.
import { availableParallelism } from 'node:os';
import { Worker } from 'node:worker_threads';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const jitter = (maxMs) => Math.floor(Math.random() * Math.max(0, maxMs));
const clock = () => performance.timeOrigin + performance.now();

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
  return { groups, degrees: ids.map((id) => adjacency.get(id).size) };
}

/**
 * Run the test. Returns { ids, mesh, broadcast, direct, ... } for the report.
 * onEvent(line) gets one line per event as it happens; verbose adds every
 * single broadcast delivery (otherwise a broadcast is reported when it has
 * reached everyone).
 */
export async function runMessageTest({
  target, peers: count, timeS = 120, waitS = 20, joinWindowS = 20, sendSpreadS = 10,
  threads = availableParallelism(), verbose = false, onEvent = () => {},
}) {
  const { minPeers: minLinks, maxPeers: maxLinks, tolerantPeers } = target.topology;
  const started = clock();
  const rel = (at) => (at - started) / 1000;
  const deadlineAt = started + timeS * 1000;
  const run = Date.now().toString(36);
  const urls = target.signalUrls?.length ? target.signalUrls : [target.signalUrl];
  const threadCount = Math.max(1, Math.min(count, Math.floor(threads) || 1));

  const plan = Array.from({ length: count }, (_, i) => ({
    i,
    joinDelayMs: jitter(joinWindowS * 1000),
    sendDelaysMs: [jitter(sendSpreadS * 1000), jitter(sendSpreadS * 1000)],
    targetIndex: (i + Math.floor(count / 2)) % count,
    signalUrl: urls[i % urls.length],
  }));

  const ids = new Array(count).fill('');
  const indexOf = new Map();
  const joinedAt = new Array(count).fill(null);
  const sides = Array.from({ length: count }, () => new Set()); // links each peer itself reports
  const messages = new Map(); // token -> { token, from, via, sentAt, to: Set, beforeMesh, ... }
  const deliveries = new Map(); // token -> Map(peer index -> at)
  let formedAt = null;
  let groupsNow = 0;
  let done = 0;
  let ending = false; // shutting the peers down drops every link: that is not news
  const short = (i) => (ids[i] || `#${i}`).slice(0, 6);
  const say = (at, text) => onEvent(`[${rel(at).toFixed(3).padStart(7)}s] ${text}`);

  const edgeUp = (a, b) => sides[a].has(ids[b]) || sides[b].has(ids[a]);
  const checkMesh = (at) => {
    const live = ids.filter(Boolean);
    const shape = meshShape(live, (id) => [...sides[indexOf.get(id)]]);
    const joined = joinedAt.filter((value) => value !== null).length;
    if (shape.groups.length !== groupsNow && joined > 1) {
      if (groupsNow) say(at, shape.groups.length === 1 ? `mesh is one group of ${joined}` : `mesh is ${shape.groups.length} groups (${shape.groups.join(', ')})`);
      groupsNow = shape.groups.length;
    }
    if (formedAt === null && joined === count && shape.groups.length === 1 && Math.min(...shape.degrees) >= Math.min(minLinks, count - 1)) {
      formedAt = rel(at);
      say(at, `mesh formed: all ${count} peers, one group, each with ${Math.min(minLinks, count - 1)}+ links`);
    }
  };

  const workers = [];
  const finals = [];
  const onMessage = (message) => {
    if (message.type === 'peer-done') { done += 1; return; }
    if (message.type === 'final') { finals.push(message); return; }
    if (message.type !== 'event' || ending) return;
    const { at, kind, i } = message;
    if (kind === 'joined') {
      ids[i] = message.id;
      indexOf.set(message.id, i);
      joinedAt[i] = at;
      for (const w of workers) w.postMessage({ type: 'ids', ids: [[i, message.id]] });
      say(at, `${short(i)} joined`);
      checkMesh(at);
    } else if (kind === 'link-up' || kind === 'link-down') {
      const j = indexOf.get(message.peer);
      const before = j !== undefined && edgeUp(i, j);
      if (kind === 'link-up') sides[i].add(message.peer); else sides[i].delete(message.peer);
      const after = j !== undefined && edgeUp(i, j);
      if (j !== undefined && before !== after) say(at, `${short(Math.min(i, j))} ${after ? '↔' : '✕'} ${short(Math.max(i, j))} link ${after ? 'up' : 'down'}`);
      checkMesh(at);
    } else if (kind === 'sent') {
      const to = message.via === 'direct' ? new Set([message.to]) : new Set(plan.map((p) => p.i).filter((j) => j !== i));
      messages.set(message.token, {
        token: message.token, from: i, via: message.via, sent: message.ok, sentAt: at, to,
        beforeMesh: formedAt === null, toIndex: message.to, neighbour: Boolean(message.neighbour),
      });
      if (!deliveries.has(message.token)) deliveries.set(message.token, new Map());
      say(at, message.via === 'direct'
        ? `${short(i)} → ${short(message.to)} direct sent${message.ok ? '' : ' (refused)'}${message.neighbour ? ' (a neighbour)' : ''}`
        : `${short(i)} broadcast sent${message.ok ? '' : ' (refused)'}`);
    } else if (kind === 'received') {
      if (!deliveries.has(message.token)) deliveries.set(message.token, new Map());
      const got = deliveries.get(message.token);
      if (got.has(i)) return;
      got.set(i, at);
      const sent = messages.get(message.token);
      const ms = sent ? `${Math.round(at - sent.sentAt)} ms` : 'before its send was reported';
      const from = Number(message.token.split(':')[2]);
      if (message.via === 'direct') say(at, `${short(i)} got ${short(from)}'s direct message (${ms})`);
      else if (verbose) say(at, `${short(i)} got ${short(from)}'s broadcast (${ms})`);
      if (message.via === 'broadcast' && sent && got.size === sent.to.size) say(at, `${short(from)}'s broadcast reached all ${got.size} (${ms} after it was sent)`);
    } else if (kind === 'error') {
      say(at, `${short(i)} error: ${message.message}`);
    }
  };

  for (let t = 0; t < threadCount; t += 1) {
    const share = plan.filter((p) => p.i % threadCount === t);
    const worker = new Worker(new URL('./message-worker.js', import.meta.url), {
      workerData: { target: { network: target.network, room: target.room, topology: target.topology }, plan: share, deadlineAt, run },
    });
    worker.on('message', onMessage);
    worker.on('error', (error) => onEvent(`worker ${t} failed: ${error?.message ?? error}`));
    workers.push(worker);
  }
  onEvent(`${count} peers on ${threadCount} thread${threadCount === 1 ? '' : 's'}; joins spread over ${joinWindowS}s, sends up to ${sendSpreadS}s apart`);

  // Until every peer has joined and sent, then until everything arrived or waitS after the last send.
  while (done < count && clock() < deadlineAt + waitS * 1000) await sleep(100);
  const lastSend = Math.max(0, ...[...messages.values()].map((m) => m.sentAt));
  const complete = () => [...messages.values()].every((m) => [...m.to].every((j) => deliveries.get(m.token)?.has(j)));
  while (!complete() && clock() < lastSend + waitS * 1000) await sleep(100);

  ending = true;
  for (const worker of workers) worker.postMessage({ type: 'final' });
  const finalDeadline = clock() + 15_000;
  while (finals.length < workers.length && clock() < finalDeadline) await sleep(50);
  await Promise.all(workers.map((worker) => worker.terminate().catch(() => {})));

  const finalLinks = new Map();
  for (const final of finals) for (const peer of final.peers) finalLinks.set(peer.id, peer.links);
  const liveIds = ids.filter(Boolean);
  const shape = meshShape(liveIds, (id) => finalLinks.get(id) ?? []);
  const own = ids.map((id) => (finalLinks.get(id) ?? []).length);
  const settle = (m) => {
    const got = deliveries.get(m.token) ?? new Map();
    const arrived = [...m.to].filter((j) => got.has(j));
    // Receivers that joined after this was sent can only get it by anti-entropy repair.
    const lateReceivers = [...m.to].filter((j) => joinedAt[j] !== null && joinedAt[j] > m.sentAt);
    return { ...m, arrived, lateReceivers, lateArrived: lateReceivers.filter((j) => got.has(j)), latencies: arrived.map((j) => got.get(j) - m.sentAt) };
  };
  const all = [...messages.values()].sort((a, b) => a.from - b.from);
  const werift = finals.reduce((sum, f) => ({ count: sum.count + (f.werift?.count ?? 0), last: f.werift?.last ?? sum.last }), { count: 0, last: null });
  return {
    ids,
    mode: 'async',
    threads: threadCount,
    joinWindowS,
    sendSpreadS,
    mesh: { formedAt, groups: shape.groups, degrees: shape.degrees, own, minLinks, maxLinks, tolerantPeers },
    broadcast: all.filter((m) => m.via === 'broadcast').map(settle),
    direct: all.filter((m) => m.via === 'direct').map(settle),
    werift,
    seconds: rel(clock()),
  };
}
