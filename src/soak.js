// wurl --messages --soak: keep a PeerPigeon mesh up for a long time and test it
// in rounds. The peers (one Node process each, as in --messages) join at
// random times and then stay. Every --round seconds each peer sends one
// broadcast and one direct message (to a peer that changes round to round),
// and wurl reports how many arrived. Once a minute it reports the mesh: how
// many of those seconds it was one group, the most groups it split into, and
// how many links went down.
//
// Several wurls on several machines can soak the same room: each counts the
// other's peers as part of the mesh (see meshShape) and tests its own peers'
// messages.
import { fork } from 'node:child_process';
import { meshShape } from './messages.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const wallClock = () => performance.timeOrigin + performance.now(); // comparable across processes
const jitter = (maxMs) => Math.floor(Math.random() * Math.max(0, maxMs));
const GRAPH_FRESH_MS = 12_000;

export const clock = (seconds) => {
  const s = Math.max(0, Math.floor(seconds));
  return [Math.floor(s / 3600), Math.floor(s / 60) % 60, s % 60].map((n) => String(n).padStart(2, '0')).join(':');
};

const percentile = (list, q) => {
  if (!list.length) return null;
  const sorted = [...list].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
};

export async function runSoakTest({
  target, peers: count, soakS, roundS = 300, waitS = 30, joinWindowS = 20, sendSpreadS = 10,
  webrtc = 'auto', onLine = () => {},
}) {
  const started = wallClock();
  const elapsed = () => (wallClock() - started) / 1000;
  const urls = target.signalUrls?.length ? target.signalUrls : [target.signalUrl];
  const run = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`; // unique even for wurls started together
  // A round's messages are counted until the next round starts, at the latest.
  const spreadS = Math.max(0, Math.min(sendSpreadS, (roundS - 5) / 3));
  const countForS = Math.max(5, Math.min(waitS + 2 * spreadS, roundS - 1));

  const ids = new Array(count).fill('');
  const indexOf = new Map();
  const sides = Array.from({ length: count }, () => new Set());
  const graphs = new Map();
  const finals = new Set();
  const exited = new Set();
  const rounds = [];             // { round, at, peers: [indices], sent: Map token -> msg, reported }
  const deliveries = new Map();  // token -> Map(peer index -> at)
  let ending = false;
  let linkDowns = 0;
  let firstFormedAt = null;

  const outsideLinks = () => {
    const cutoff = wallClock() - GRAPH_FRESH_MS;
    const pairs = [];
    for (const { at, edges } of graphs.values()) if (at >= cutoff) pairs.push(...edges);
    return pairs;
  };

  const children = Array.from({ length: count }, (_, i) => {
    const child = fork(new URL('./message-peer.js', import.meta.url), [], {
      env: {
        ...process.env,
        WURL_PEER: JSON.stringify({
          i,
          target: { network: target.network, room: target.room, topology: target.topology },
          joinDelayMs: jitter(joinWindowS * 1000),
          sendDelaysMs: [0, 0],
          targetIndex: (i + 1) % count,
          signalUrl: urls[i % urls.length],
          run,
          engine: webrtc,
        }),
      },
      stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
    });
    child.on('message', (message) => {
      if (message?.type === 'final') { finals.add(i); return; }
      if (message?.type !== 'event' || ending) return;
      const { at, kind } = message;
      if (kind === 'joined') {
        ids[i] = message.id;
        indexOf.set(message.id, i);
      } else if (kind === 'link-up') {
        sides[i].add(message.peer);
      } else if (kind === 'link-down') {
        sides[i].delete(message.peer);
        linkDowns += 1;
      } else if (kind === 'graph') {
        graphs.set(i, { at: wallClock(), edges: Array.isArray(message.edges) ? message.edges : [] });
      } else if (kind === 'sent') {
        const r = rounds[message.round];
        if (!r) return;
        const to = message.via === 'direct' ? [message.to] : r.peers.filter((j) => j !== i);
        r.sent.set(message.token, { from: i, via: message.via, ok: message.ok, sentAt: at, to: new Set(to) });
      } else if (kind === 'received') {
        // Kept by token, not by round: each peer reports from its own
        // process, so a neighbour's 'received' can reach us before the
        // sender's 'sent' does.
        if (!deliveries.has(message.token)) deliveries.set(message.token, new Map());
        if (!deliveries.get(message.token).has(i)) deliveries.get(message.token).set(i, at);
      } else if (kind === 'error') {
        onLine(`peer ${i} error: ${message.message}`);
      }
    });
    child.on('exit', (code) => {
      exited.add(i);
      if (!ending) onLine(`[${clock(elapsed())}] peer ${i} exited (code ${code})`);
    });
    return child;
  });

  const shapeNow = () => {
    const live = ids.filter(Boolean);
    return meshShape(live, (id) => [...sides[indexOf.get(id)]], outsideLinks());
  };

  const scoreRound = (r) => {
    let bWant = 0; let bGot = 0; let dWant = 0; let dGot = 0;
    const latencies = [];
    const missed = [];
    for (const [token, m] of r.sent) {
      const got = deliveries.get(token) ?? new Map();
      for (const j of m.to) {
        const at = got.get(j);
        if (m.via === 'broadcast') { bWant += 1; if (at !== undefined) bGot += 1; } else { dWant += 1; if (at !== undefined) dGot += 1; }
        if (at !== undefined) latencies.push(at - m.sentAt);
        else if (m.via === 'direct') missed.push(`${m.from}->${j}`);
      }
    }
    return { bWant, bGot, dWant, dGot, latencies, missed, sent: r.sent.size, expected: r.peers.length * 2 };
  };

  // Minute stats.
  let minuteStart = 0;
  let minuteOneGroup = 0;
  let minuteSeconds = 0;
  let minuteMaxGroups = 0;
  let minuteDowns = 0;
  let totalSeconds = 0;
  let totalOneGroup = 0;
  let maxGroupsEver = 0;
  let nextRoundAt = null;
  let lastTick = -1;
  const results = [];

  const pct = (got, want) => (want ? `${((100 * got) / want).toFixed(1)}%` : '-');

  while (elapsed() < soakS && exited.size < count) {
    const t = elapsed();
    const second = Math.floor(t);
    if (second !== lastTick) {
      lastTick = second;
      const shape = shapeNow();
      const joined = ids.filter(Boolean).length;
      const oneGroup = joined === count && shape.groups.length === 1;
      if (oneGroup && firstFormedAt === null) {
        firstFormedAt = t;
        onLine(`[${clock(t)}] mesh formed: one group of ${count} wurl peers${shape.outside ? ` and ${shape.outside} that are not wurl's` : ''}`);
      }
      if (firstFormedAt !== null) {
        minuteSeconds += 1; totalSeconds += 1;
        if (oneGroup) { minuteOneGroup += 1; totalOneGroup += 1; }
        minuteMaxGroups = Math.max(minuteMaxGroups, shape.groups.length);
        maxGroupsEver = Math.max(maxGroupsEver, shape.groups.length);
      }
      if (t - minuteStart >= 60) {
        const degrees = shape.degrees.length ? shape.degrees : [0];
        onLine(`[${clock(t)}] mesh: ${joined}/${count} joined, ${shape.outside} not wurl's; one group ${minuteOneGroup}/${minuteSeconds} s this minute (most ${minuteMaxGroups} group${minuteMaxGroups === 1 ? '' : 's'}); links min ${Math.min(...degrees)} avg ${(degrees.reduce((a, b) => a + b, 0) / degrees.length).toFixed(1)} max ${Math.max(...degrees)}; ${linkDowns - minuteDowns} links went down`);
        minuteStart = t; minuteOneGroup = 0; minuteSeconds = 0; minuteMaxGroups = 0; minuteDowns = linkDowns;
      }
      // The first round once everyone has joined and had time to link up,
      // whether or not the mesh is one group: a split mesh is what a round
      // should show.
      if (nextRoundAt === null && (firstFormedAt !== null || t >= joinWindowS + 60)) nextRoundAt = t + 5;
      if (nextRoundAt !== null && t >= nextRoundAt && t + countForS < soakS) {
        const round = rounds.length;
        const peersNow = ids.map((id, j) => (id && !exited.has(j) ? j : -1)).filter((j) => j >= 0);
        rounds.push({ round, at: t, peers: peersNow, sent: new Map(), reported: false });
        for (const j of peersNow) { try { children[j].send({ type: 'round', round, ids, spreadMs: spreadS * 1000 }); } catch {} }
        nextRoundAt = t + roundS;
      }
      for (const r of rounds) {
        if (r.reported || t < r.at + countForS) continue;
        r.reported = true;
        const s = scoreRound(r);
        results.push({ round: r.round, at: r.at, ...s, oneGroupAtEnd: shape.groups.length === 1 });
        const median = percentile(s.latencies, 0.5); const p95 = percentile(s.latencies, 0.95);
        onLine(`[${clock(t)}] round ${r.round + 1}: broadcast ${s.bGot}/${s.bWant} (${pct(s.bGot, s.bWant)}), direct ${s.dGot}/${s.dWant} (${pct(s.dGot, s.dWant)}); ${s.sent}/${s.expected} sent${s.latencies.length ? `; median ${Math.round(median)} ms, p95 ${Math.round(p95)} ms` : ''}${s.missed.length ? `; direct not delivered: ${s.missed.slice(0, 8).join(', ')}${s.missed.length > 8 ? ', ...' : ''}` : ''}`);
      }
    }
    await sleep(200);
  }

  ending = true;
  for (const child of children) { try { child.send({ type: 'final' }); } catch {} }
  const finalDeadline = wallClock() + 15_000;
  while (finals.size + [...exited].filter((i) => !finals.has(i)).length < count && wallClock() < finalDeadline) await sleep(50);
  for (const child of children) { try { child.kill(); } catch {} }

  const sum = (key) => results.reduce((total, r) => total + r[key], 0);
  return {
    soakS, roundS, ranS: elapsed(), peers: count, firstFormedAt,
    oneGroupSeconds: totalOneGroup, measuredSeconds: totalSeconds, maxGroups: maxGroupsEver, linkDowns,
    rounds: results,
    broadcast: { got: sum('bGot'), want: sum('bWant') },
    direct: { got: sum('dGot'), want: sum('dWant') },
    exited: exited.size,
  };
}
