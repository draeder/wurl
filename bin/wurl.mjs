#!/usr/bin/env node
// wurl: curl for WebRTC. Join a PSP site's network and room as a browserless
// peer and report every step: relay, registration, discovery, WebRTC
// negotiation, data channel, round trips.
import { parseArgs } from 'node:util';
import { containWeriftErrors, installWebRTC, weriftErrors } from '../src/webrtc.js';
import { resolveTarget } from '../src/target.js';
import { startPeer } from '../src/peer.js';
import { costDelta, formatCost, readCost } from '../src/cost.js';

function isLocalRelay(url) {
  try {
    const host = new URL(url).hostname.replace(/^\[|\]$/g, '').toLowerCase();
    // .test is reserved (RFC 2606): never a public relay, so test hostnames count as local.
    return host === 'localhost' || host.endsWith('.localhost') || host.endsWith('.local') || host.endsWith('.test') || host === '::1'
      || host === 'host.docker.internal' || /^127\./.test(host) || /^10\./.test(host) || /^192\.168\./.test(host)
      || /^172\.(1[6-9]|2\d|3[01])\./.test(host);
  } catch {
    return false;
  }
}

// Many peers against a public relay can use up its whole daily request limit
// (a 50-peer test spent the free plan's 100,000 in an hour). Up to this many
// is a test; more needs --public-load, or a local relay via --signal.
const PUBLIC_PEER_LIMIT = 5;

const HELP = `usage: wurl [options] <url>

  <url>  a PSP site, e.g. "https://decentralize.ooo/?network=network:test&room=room-1",
         or a relay URL (wss://host/ws). network and room come from the URL's
         ?network= and ?room= (also networkName / sessionId), else network:test / room:test.

options:
  -n, --network <name>   network to join (overrides the URL)
  -r, --room <id>        room to join (overrides the URL)
      --signal <url>     relay to use instead of the one the URL implies; a comma-separated
                         list spreads the peers over several relays (peer i uses the i-th, round robin)
      --cost <urls>      comma-separated Node relays (freertc scripts/node-relay.mjs) to read
                         /debug/cost from before and after the run: the Worker requests,
                         Durable Object requests and messages, and D1 queries the run cost them,
                         as Cloudflare would bill the same relay code
  -p, --peers <n>        run n peers in this process (default 1, or 10 with --messages); they also
                         connect to each other.
                         More than ${PUBLIC_PEER_LIMIT} against a public relay needs --public-load
      --public-load      allow more than ${PUBLIC_PEER_LIMIT} peers against a public relay (each peer's
                         signaling counts against that relay's daily request limit)
  -c, --connect <n>      connections each peer dials, at most (default 4)
  -t, --time <s>         longest run, in seconds (default 20); wurl stops sooner once
                         every peer has settled (connected or failed, round trips measured)
  -d, --data <text>      send this on each data channel when it opens
  -v, --verbose          also show freertc's own signaling and WebRTC log
  -s, --silent           only the summary
  -m, --messages         PeerPigeon test: every peer broadcasts once and sends one direct message
                         to a peer it is usually not linked to; reports delivery and latency.
                         Peers join at their own random times; once the mesh is formed and settled
                         (one group, minPeers links each, held for --settle), each sends at its own
                         random times
      --join-window <s>  with --messages: peers join at random times within this (default 20)
      --send-spread <s>  with --messages: each send waits a random 0..s after the last (default 10)
      --phased           with --messages: the old lockstep test instead (all join at once, the mesh
                         forms and holds for --settle, then all broadcasts, then all direct messages)
                         The topology comes from the URL, as on PeerPigeon's page:
                         ?topology=token-ring|star|partial-mesh|dense-mesh (default token-ring),
                         or ?minPeers= ?maxPeers= ?tolerantPeers= directly
      --min-links <n>    with --messages: override minPeers
      --max-links <n>    with --messages: override maxPeers
      --tolerant <n>     with --messages: override tolerantPeers
      --wait <s>         with --messages: how long to wait for messages to arrive (default 20)
      --settle <s>       with --messages: how long the formed mesh must hold steady before anyone sends (default 3)
      --webrtc <engine>  with --messages: native (node-datachannel, C++), werift (JavaScript), or auto:
                         native when node-datachannel is installed, else werift (default auto).
                         Each peer runs in its own Node process
      --json             the summary as JSON
  -h, --help

exit status: 0 every peer registered and connected to at least one other peer
  (with --messages: one mesh, and every broadcast and direct message arrived; else 1);
  6 the relay couldn't be reached; 7 signaling failed before registering;
  52 registered, but nobody else is in the room; 28 peers were found but none connected.
`;

let args;
try {
  args = parseArgs({
    allowPositionals: true,
    options: {
      network: { type: 'string', short: 'n' },
      room: { type: 'string', short: 'r' },
      signal: { type: 'string' },
      peers: { type: 'string', short: 'p' },
      messages: { type: 'boolean', short: 'm', default: false },
      'min-links': { type: 'string' },
      'max-links': { type: 'string' },
      tolerant: { type: 'string' },
      wait: { type: 'string', default: '20' },
      settle: { type: 'string', default: '3' },
      phased: { type: 'boolean', default: false },
      'join-window': { type: 'string', default: '20' },
      webrtc: { type: 'string', default: 'auto' },
      'send-spread': { type: 'string', default: '10' },
      connect: { type: 'string', short: 'c', default: '4' },
      time: { type: 'string', short: 't', default: '20' },
      data: { type: 'string', short: 'd' },
      verbose: { type: 'boolean', short: 'v', default: false },
      silent: { type: 'boolean', short: 's', default: false },
      json: { type: 'boolean', default: false },
      'public-load': { type: 'boolean', default: false },
      cost: { type: 'string' },
      help: { type: 'boolean', short: 'h', default: false },
    },
  });
} catch (error) {
  console.error(`wurl: ${error.message}\n\n${HELP}`);
  process.exit(2);
}
const { values: opt, positionals } = args;
if (opt.help || positionals.length !== 1) {
  (opt.help ? console.log : console.error)(HELP);
  process.exit(opt.help ? 0 : 2);
}

let target;
try {
  target = resolveTarget(positionals[0], {
    network: opt.network, room: opt.room, signal: opt.signal,
    minPeers: opt['min-links'], maxPeers: opt['max-links'], tolerantPeers: opt.tolerant,
  });
} catch (error) {
  console.error(`wurl: ${error.message}`);
  process.exit(2);
}

const PEERS = Math.max(opt.messages ? 2 : 1, Number.parseInt(opt.peers ?? '', 10) || (opt.messages ? 10 : 1));
const CONNECT = Math.max(1, Number.parseInt(opt.connect, 10) || 4);
const TIME_S = Math.max(1, Number(opt.time) || 20);
const quiet = opt.silent || opt.json;

if (PEERS > PUBLIC_PEER_LIMIT && !target.signalUrls.every(isLocalRelay) && !opt['public-load']) {
  console.error(`wurl: ${PEERS} peers against the public relay ${target.signalUrl} would count against its daily request limit.`);
  console.error(`  Run them against a local relay instead:  --signal ws://127.0.0.1:8788/ws`);
  console.error(`  (start one with: node scripts/node-relay.mjs in freertc), or add --public-load to do it anyway.`);
  process.exit(2);
}
const many = PEERS > 1;
const short = (peerId) => String(peerId || '').slice(0, 8) || '?';
const t = (seconds) => `${seconds.toFixed(2)}s`;
// The status line redraws in place on a terminal; piped or redirected, each
// update is its own line, so a log still reads in order.
const LIVE = process.stderr.isTTY === true;
let statusLine = null;
const fit = (line) => {
  const columns = process.stderr.columns || 0;
  return columns > 1 && line.length >= columns ? `${line.slice(0, columns - 2)}…` : line;
};
const say = (line) => {
  if (quiet) return;
  if (LIVE && statusLine !== null) process.stderr.write('\r\x1b[K');
  process.stderr.write(`${line}\n`);
  if (LIVE && statusLine !== null) process.stderr.write(fit(statusLine));
};
const status = (line) => {
  if (quiet) return;
  if (!LIVE) { process.stderr.write(`${line}\n`); return; }
  statusLine = line;
  process.stderr.write(`\r\x1b[K${fit(line)}`);
};
const endStatus = () => {
  if (LIVE && statusLine !== null && !quiet) process.stderr.write('\n');
  statusLine = null;
};

await installWebRTC();
containWeriftErrors();

const COST_URLS = opt.cost ? opt.cost.split(',').map((url) => url.trim()).filter(Boolean) : [];
const costBefore = COST_URLS.length ? await readCost(COST_URLS) : null;
async function reportCost({ peers, links, json }) {
  if (!costBefore) return null;
  // Let in-flight federation work (provider publishes, queued forwards) land first.
  await new Promise((resolve) => setTimeout(resolve, 1500));
  const rows = costDelta(costBefore, await readCost(COST_URLS));
  if (json) console.log(JSON.stringify({ cost: rows, peers, links }, null, 2));
  else for (const line of formatCost(rows, { peers, links })) console.log(line);
  return rows;
}

if (opt.messages) {
  const { runMessageTest } = await import('../src/messages.js');
  const tp = target.topology;
  say(`* wurl --messages: ${PEERS} PeerPigeon peers on ${target.signalUrls.join(', ')}, network ${target.network}, room ${target.room}`);
  say(`* topology ${tp.name}: minPeers ${tp.minPeers}, maxPeers ${tp.maxPeers}, tolerantPeers ${tp.tolerantPeers} (from ${tp.source})`);
  say(opt.phased
    ? `* phased: all join at once, the mesh must hold for ${opt.settle}s, then all broadcasts, then all direct messages`
    : `* peers join at random times within ${opt['join-window']}s; once the mesh is formed and has held for ${opt.settle}s, each sends at its own random times (0..${opt['send-spread']}s apart)`);
  const result = await runMessageTest({
    target,
    peers: PEERS,
    timeS: TIME_S,
    waitS: Math.max(1, Number(opt.wait) || 20),
    settleS: Math.max(0, Number(opt.settle ?? 3)),
    phased: opt.phased,
    joinWindowS: Math.max(0, Number(opt['join-window'] ?? 20)),
    sendSpreadS: Math.max(0, Number(opt['send-spread'] ?? 10)),
    webrtc: ['native', 'werift', 'auto'].includes(opt.webrtc) ? opt.webrtc : 'auto',
    onProgress: (line) => (/^\[\d/.test(line) ? status(`* ${line}`) : say(`* ${line}`)),
  });
  const ms = (list) => {
    if (!list.length) return '-';
    const sorted = [...list].sort((a, b) => a - b);
    const at = (q) => Math.round(sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]);
    return `median ${at(0.5)} ms, p95 ${at(0.95)} ms, max ${Math.round(sorted[sorted.length - 1])} ms`;
  };
  endStatus();
  const { mesh, broadcast, direct, ids } = result;
  const n = ids.length;
  const meshOk = mesh.groups.length === 1 && mesh.groups[0] === n;
  const fullB = broadcast.filter((m) => m.arrived.length === m.to.size).length;
  const deliveredB = broadcast.reduce((sum, m) => sum + m.arrived.length, 0);
  const totalB = broadcast.reduce((sum, m) => sum + m.to.size, 0);
  const deliveredD = direct.filter((m) => m.arrived.length === 1);
  const routedD = direct.filter((m) => !m.neighbour).length;
  const overLimit = mesh.own.filter((c) => c > mesh.maxLinks + mesh.tolerantPeers).length;
  const ok = meshOk && fullB === n && deliveredD.length === n && overLimit === 0;
  if (opt.json) {
    console.log(JSON.stringify({
      relay: target.signalUrl, network: target.network, room: target.room, peers: n, ok, topology: target.topology, mode: result.mode,
      mesh,
      broadcast: { senders_complete: fullB, delivered: deliveredB, expected: totalB, latencies_ms: broadcast.flatMap((m) => m.latencies.map(Math.round)) },
      direct: { delivered: deliveredD.length, expected: n, routed: routedD, failed: direct.filter((m) => m.arrived.length === 0).map((m) => ({ from: ids[m.from], to: ids[m.toIndex], sent: m.sent })) },
    }, null, 2));
  } else {
    const short = (i) => (ids[i] || '?').slice(0, 8);
    const links = mesh.degrees.length ? mesh.degrees : [0];
    console.log('');
    const own = mesh.own.length ? mesh.own : [0];
    const over = mesh.own.filter((c) => c > mesh.maxLinks).length;
    const overTolerant = mesh.own.filter((c) => c > mesh.maxLinks + mesh.tolerantPeers).length;
    console.log(`topology   ${target.topology.name}: minPeers ${mesh.minLinks}, maxPeers ${mesh.maxLinks}, tolerantPeers ${mesh.tolerantPeers} (from ${target.topology.source})`);
    console.log(`links      each peer's own count min ${Math.min(...own)}, avg ${(own.reduce((a, b) => a + b, 0) / own.length).toFixed(1)}, max ${Math.max(...own)}; ${over ? `${over} over maxPeers${overTolerant ? `, ${overTolerant} over maxPeers + tolerantPeers` : ''}` : 'none over maxPeers'}`);
    console.log(`mesh       ${n} peers: ${meshOk ? 'one group' : `${mesh.groups.length} groups (${mesh.groups.join(', ')})`}${mesh.formedAt === null ? ', never fully formed' : `, formed in ${mesh.formedAt.toFixed(1)}s`}; counting a link if either end lists it: min ${Math.min(...links)}, avg ${(links.reduce((a, b) => a + b, 0) / links.length).toFixed(1)}, max ${Math.max(...links)}`);
    if (result.mode === 'async') {
      const early = [...broadcast, ...direct].filter((m) => m.beforeMesh).length;
      const late = broadcast.reduce((sum, m) => sum + m.lateReceivers.length, 0);
      const lateGot = broadcast.reduce((sum, m) => sum + m.lateArrived.length, 0);
      if (result.processes) console.log(`peers      ${result.processes} Node processes, one per peer; WebRTC ${Object.entries(result.engines || {}).map(([engine, n]) => `${engine} ${n}`).join(', ') || 'unknown'}`);
      console.log(`async      joins spread over ${result.joinWindowS}s, all ${n} joined at ${result.allJoinedAt === null ? 'never' : `${result.allJoinedAt.toFixed(1)}s`} and all had a link at ${result.allLinkedAt === null ? 'never' : `${result.allLinkedAt.toFixed(1)}s`}; mesh settled at ${result.settledAt === null ? 'never' : `${result.settledAt.toFixed(1)}s`} (held ${result.settleS}s); then sends at random times up to ${result.sendSpreadS}s apart; ${early}/${broadcast.length + direct.length} messages sent before the mesh settled`);
    }
    console.log(`broadcast  ${fullB}/${n} reached all ${n - 1} others; ${deliveredB}/${totalB} deliveries (${totalB ? ((deliveredB / totalB) * 100).toFixed(1) : 0}%); ${ms(broadcast.flatMap((m) => m.latencies))}`);
    const missed = ids.map((_, i) => ({ i, missed: broadcast.filter((m) => m.to.has(i) && !m.arrived.includes(i)).length })).filter((r) => r.missed).sort((a, b) => b.missed - a.missed);
    if (missed.length) console.log(`           missed broadcasts: ${missed.slice(0, 10).map((r) => `${short(r.i)} ${r.missed}`).join(', ')}${missed.length > 10 ? ` (+${missed.length - 10} more)` : ''}`);
    console.log(`direct     ${deliveredD.length}/${n} delivered (${routedD} routed through the mesh, ${n - routedD} to a neighbour); ${ms(direct.flatMap((m) => m.latencies))}`);
    for (const m of direct.filter((d) => d.arrived.length === 0).slice(0, 10)) console.log(`           ${short(m.from)} -> ${short(m.toIndex)} not delivered${m.sent ? '' : ' (send refused)'}`);
    { const errors = result.werift ?? weriftErrors; if (errors.count) console.log(`werift     ${errors.count} internal error(s) contained, e.g. ${errors.last}`); }
    console.log(`result     ${ok ? 'OK' : 'FAILED'} (${result.seconds.toFixed(1)}s)`);
  }
  await reportCost({ peers: n, links: Math.round(mesh.own.reduce((a, b) => a + b, 0) / 2), json: opt.json });
  setTimeout(() => process.exit(ok ? 0 : 1), 300).unref();
  process.exitCode = ok ? 0 : 1;
} else {

say(`* wurl ${target.site}`);
say(`* relay ${target.signalUrls.join(', ')}, network ${target.network}, room ${target.room}${many ? `, ${PEERS} peers` : ''}`);

const peers = [];
let lastChange = performance.now();
const onEvent = (event) => {
  if (event.type !== 'log' && event.type !== 'rtt') lastChange = performance.now();
  if (quiet || (many && !opt.verbose)) return;
  const who = many ? `${short(event.peer)} ` : '';
  const stamp = `[${t(event.at)}]`;
  switch (event.type) {
    case 'status':
      if (event.status === 'connecting') say(`* ${stamp} ${who}connecting to the relay`);
      else if (event.status === 'connected') say(`* ${stamp} ${who}relay connected`);
      else if (event.status === 'error') say(`* ${stamp} ${who}relay error`);
      break;
    case 'registered': say(`* ${stamp} ${who}registered and announced in ${target.network} / ${target.room}`); break;
    case 'peers': say(`* ${stamp} ${who}found ${event.peers.length} peer(s): ${event.peers.map(short).join(' ')} (${event.total} so far)`); break;
    case 'dial': say(`> ${stamp} ${who}offer to ${short(event.to)}`); break;
    case 'connected': say(`* ${stamp} ${who}data channel open with ${short(event.to)}${event.incoming ? ' (they dialed us)' : ` after ${t(event.took)}`}`); break;
    case 'failed': say(`! ${stamp} ${who}${event.to ? `${short(event.to)}: ` : ''}${event.reason}`); break;
    case 'closed': say(`* ${stamp} ${who}${short(event.to)} closed`); break;
    case 'sent': say(`> ${stamp} ${who}sent ${event.bytes} bytes to ${short(event.to)}`); break;
    case 'data': say(`< ${stamp} ${who}${short(event.from)}: ${event.text}${event.bytes > 200 ? '…' : ''} (${event.bytes} bytes)`); break;
    case 'log': say(`  ${stamp} ${who}${event.message}`); break;
    default: break;
  }
};

for (let i = 0; i < PEERS; i += 1) {
  peers.push(startPeer({ target: { ...target, signalUrl: target.signalUrls[i % target.signalUrls.length] }, maxConnect: CONNECT, payload: opt.data ?? null, verbose: opt.verbose, onEvent }));
}

const started = performance.now();
const elapsed = () => (performance.now() - started) / 1000;
let progress = null;
if (many && !quiet && !opt.verbose) {
  progress = setInterval(() => {
    const registered = peers.filter((p) => p.state.registeredAt !== null).length;
    const links = peers.map((p) => p.state.connected.size);
    status(`* [${t(elapsed())}] registered ${registered}/${PEERS}; links per peer min ${Math.min(...links)} avg ${(links.reduce((a, b) => a + b, 0) / PEERS).toFixed(1)} max ${Math.max(...links)}`);
  }, 2000);
}

// When to stop: at --time, or as soon as every peer has settled: registered,
// every peer it dialed connected or failed, a round trip measured over each
// link that answers pings, and nothing new for QUIET_S. A room with nobody
// else in it settles QUIET_S after registering.
const QUIET_S = 2;
const settled = ({ state }) => {
  if (state.registeredAt === null) return false;
  if (state.seen.size === 0) return elapsed() - state.registeredAt >= QUIET_S;
  const dialedDone = [...state.dialed.keys()].every((peerId) => state.connected.has(peerId) || state.failed.has(peerId));
  if (!dialedDone || state.connected.size + state.failed.size === 0) return false;
  // Other wurl peers answer pings; browsers usually don't, so a link to one only
  // has to have been open a moment.
  const localLinks = [...state.connected.keys()].filter((peerId) => localIds.has(peerId));
  return localLinks.every((peerId) => state.rtt.has(peerId));
};
const localIds = new Set(peers.map((p) => p.id));
await new Promise((resolve) => {
  const check = setInterval(() => {
    const quietFor = (performance.now() - lastChange) / 1000;
    if (elapsed() >= TIME_S || (quietFor >= QUIET_S && peers.every(settled))) {
      clearInterval(check);
      resolve();
    }
  }, 100);
});
clearInterval(progress);
endStatus();

// Summary.
const results = peers.map((p) => ({
  id: p.id,
  relay_connected_s: p.state.signalingAt,
  registered_s: p.state.registeredAt,
  peers_found: p.state.seen.size,
  connected: [...p.state.connected.entries()].map(([peerId, s]) => ({
    peer: peerId,
    at_s: s,
    took_s: p.state.dialed.has(peerId) ? s - p.state.dialed.get(peerId) : null,
    rtt_ms: p.state.rtt.has(peerId) ? Math.round(p.state.rtt.get(peerId)) : null,
    local: localIds.has(peerId),
  })),
  failed: [...p.state.failed.entries()].map(([peerId, reason]) => ({ peer: peerId, reason })),
  received: p.state.received.length,
}));

let exit = 0;
const registeredCount = results.filter((r) => r.registered_s !== null).length;
const relayCount = results.filter((r) => r.relay_connected_s !== null).length;
const connectedCount = results.filter((r) => r.connected.length > 0).length;
if (relayCount === 0) exit = 6;
else if (registeredCount < PEERS) exit = 7;
else if (results.every((r) => r.peers_found === 0)) exit = 52;
else if (connectedCount < PEERS) exit = 28;

if (opt.json) {
  console.log(JSON.stringify({ site: target.site, relay: target.signalUrl, network: target.network, room: target.room, seconds: elapsed(), exit, peers: results }, null, 2));
} else if (!many) {
  const r = results[0];
  console.log('');
  console.log(`relay      ${target.signalUrl} ${r.relay_connected_s === null ? 'NOT REACHED' : `connected ${t(r.relay_connected_s)}`}`);
  console.log(`registered ${r.registered_s === null ? 'no' : t(r.registered_s)} in ${target.network} / ${target.room}`);
  console.log(`peers      ${r.peers_found} found, ${r.connected.length} connected, ${r.failed.length} failed`);
  for (const c of r.connected) {
    console.log(`  ${short(c.peer)}  open ${c.took_s === null ? '(incoming)' : `in ${t(c.took_s)}`}${c.rtt_ms === null ? '' : `, rtt ${c.rtt_ms} ms`}`);
  }
  for (const f of r.failed) console.log(`  ${short(f.peer)}  FAILED: ${f.reason}`);
  if (r.received) console.log(`received   ${r.received} message(s)`);
} else {
  const links = results.map((r) => r.connected.length);
  const opens = results.flatMap((r) => r.connected.map((c) => c.took_s).filter((v) => v !== null)).sort((a, b) => a - b);
  const rtts = results.flatMap((r) => r.connected.map((c) => c.rtt_ms).filter((v) => v !== null)).sort((a, b) => a - b);
  const median = (list) => (list.length ? list[Math.floor(list.length / 2)] : null);
  // Mesh among this process's peers.
  const adjacency = new Map(results.map((r) => [r.id, new Set(r.connected.filter((c) => c.local).map((c) => c.peer))]));
  for (const [id, set] of adjacency) for (const other of set) adjacency.get(other)?.add(id);
  const groups = [];
  const seen = new Set();
  for (const id of adjacency.keys()) {
    if (seen.has(id)) continue;
    const queue = [id]; seen.add(id); let size = 0;
    while (queue.length) { const next = queue.shift(); size += 1; for (const o of adjacency.get(next)) if (!seen.has(o)) { seen.add(o); queue.push(o); } }
    groups.push(size);
  }
  groups.sort((a, b) => b - a);
  const failures = results.reduce((sum, r) => sum + r.failed.length, 0);
  console.log('');
  console.log(`relay      ${target.signalUrls.join(', ')}: ${relayCount}/${PEERS} connected, ${registeredCount}/${PEERS} registered in ${target.network} / ${target.room}`);
  console.log(`links      per peer min ${Math.min(...links)}, avg ${(links.reduce((a, b) => a + b, 0) / PEERS).toFixed(1)}, max ${Math.max(...links)}; ${connectedCount}/${PEERS} peers have at least one`);
  console.log(`mesh       ${groups.length === 1 ? `one group of all ${PEERS} wurl peers` : `${groups.length} groups: ${groups.join(', ')}`}`);
  console.log(`timing     data channel open median ${median(opens) === null ? '-' : t(median(opens))}, rtt median ${median(rtts) === null ? '-' : `${Math.round(median(rtts))} ms`}`);
  if (failures) console.log(`failures   ${failures}: ${[...new Set(results.flatMap((r) => r.failed.map((f) => f.reason)))].join('; ')}`);
  if (weriftErrors.count) console.log(`werift     ${weriftErrors.count} internal error(s) contained, e.g. ${weriftErrors.last}`);
}

for (const p of peers) p.stop();
{
  const pairs = new Set(results.flatMap((r) => r.connected.map((c) => [r.id, c.peer].sort().join(':'))));
  await reportCost({ peers: PEERS, links: pairs.size, json: opt.json });
}
setTimeout(() => process.exit(exit), 300).unref();
process.exitCode = exit;
}
