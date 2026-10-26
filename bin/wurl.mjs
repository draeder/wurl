#!/usr/bin/env node
// wurl: curl for WebRTC. Join a PSP site's network and room as a browserless
// peer and report every step: relay, registration, discovery, WebRTC
// negotiation, data channel, round trips.
import { parseArgs } from 'node:util';
import { installWebRTC } from '../src/webrtc.js';
import { resolveTarget } from '../src/target.js';
import { startPeer } from '../src/peer.js';

const HELP = `usage: wurl [options] <url>

  <url>  a PSP site, e.g. "https://decentralize.ooo/?network=network:test&room=room-1",
         or a relay URL (wss://host/ws). network and room come from the URL's
         ?network= and ?room= (also networkName / sessionId), else network:test / room:test.

options:
  -n, --network <name>   network to join (overrides the URL)
  -r, --room <id>        room to join (overrides the URL)
      --signal <url>     relay to use instead of the one the URL implies
  -p, --peers <n>        run n peers in this process (default 1); they also connect to each other
  -c, --connect <n>      connections each peer dials, at most (default 4)
  -t, --time <s>         longest run, in seconds (default 20); wurl stops sooner once
                         every peer has settled (connected or failed, round trips measured)
  -d, --data <text>      send this on each data channel when it opens
  -v, --verbose          also show freertc's own signaling and WebRTC log
  -s, --silent           only the summary
      --json             the summary as JSON
  -h, --help

exit status: 0 every peer registered and connected to at least one other peer;
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
      peers: { type: 'string', short: 'p', default: '1' },
      connect: { type: 'string', short: 'c', default: '4' },
      time: { type: 'string', short: 't', default: '20' },
      data: { type: 'string', short: 'd' },
      verbose: { type: 'boolean', short: 'v', default: false },
      silent: { type: 'boolean', short: 's', default: false },
      json: { type: 'boolean', default: false },
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
  target = resolveTarget(positionals[0], { network: opt.network, room: opt.room, signal: opt.signal });
} catch (error) {
  console.error(`wurl: ${error.message}`);
  process.exit(2);
}

const PEERS = Math.max(1, Number.parseInt(opt.peers, 10) || 1);
const CONNECT = Math.max(1, Number.parseInt(opt.connect, 10) || 4);
const TIME_S = Math.max(1, Number(opt.time) || 20);
const quiet = opt.silent || opt.json;
const many = PEERS > 1;
const short = (peerId) => String(peerId || '').slice(0, 8) || '?';
const t = (seconds) => `${seconds.toFixed(2)}s`;
const say = (line) => { if (!quiet) process.stderr.write(`${line}\n`); };

await installWebRTC();
say(`* wurl ${target.site}`);
say(`* relay ${target.signalUrl}, network ${target.network}, room ${target.room}${many ? `, ${PEERS} peers` : ''}`);

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
  peers.push(startPeer({ target, maxConnect: CONNECT, payload: opt.data ?? null, verbose: opt.verbose, onEvent }));
}

const started = performance.now();
const elapsed = () => (performance.now() - started) / 1000;
let progress = null;
if (many && !quiet && !opt.verbose) {
  progress = setInterval(() => {
    const registered = peers.filter((p) => p.state.registeredAt !== null).length;
    const links = peers.map((p) => p.state.connected.size);
    say(`* [${t(elapsed())}] registered ${registered}/${PEERS}; links per peer min ${Math.min(...links)} avg ${(links.reduce((a, b) => a + b, 0) / PEERS).toFixed(1)} max ${Math.max(...links)}`);
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
  console.log(`relay      ${target.signalUrl}: ${relayCount}/${PEERS} connected, ${registeredCount}/${PEERS} registered in ${target.network} / ${target.room}`);
  console.log(`links      per peer min ${Math.min(...links)}, avg ${(links.reduce((a, b) => a + b, 0) / PEERS).toFixed(1)}, max ${Math.max(...links)}; ${connectedCount}/${PEERS} peers have at least one`);
  console.log(`mesh       ${groups.length === 1 ? `one group of all ${PEERS} wurl peers` : `${groups.length} groups: ${groups.join(', ')}`}`);
  console.log(`timing     data channel open median ${median(opens) === null ? '-' : t(median(opens))}, rtt median ${median(rtts) === null ? '-' : `${Math.round(median(rtts))} ms`}`);
  if (failures) console.log(`failures   ${failures}: ${[...new Set(results.flatMap((r) => r.failed.map((f) => f.reason)))].join('; ')}`);
}

for (const p of peers) p.stop();
setTimeout(() => process.exit(exit), 300).unref();
process.exitCode = exit;
