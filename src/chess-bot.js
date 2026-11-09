// One PigeonChess player of wurl --chess, in its own Node process, as a real player runs on its own
// machine: a PeerPigeon peer in PigeonChess's mesh, with a key of its own (kept, so it is the same
// player next time and its standings follow it), playing with Stockfish at its level.
//
// What it does is PigeonChess's own code (pigeonchess/bot): its tournament client signs it up, starts a
// tournament that is due, matches it with whoever it still owes a game, and reports each result; BotGame
// plays the game against the page (or another bot) with the messages a page uses, sealed with the game's
// seat key.
//
// To the parent: { type: 'event', kind, ... } lines to print; { type: 'standings', lines } now and then.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { register } from 'tsx/esm/api';
import { containWeriftErrors, installWebRTC } from './webrtc.js';

const config = JSON.parse(process.env.WURL_BOT);
const { i, name, level, signalUrl, network, room, join, create, engine } = config;
const report = (message) => {
  if (!process.connected) return;
  try { process.send?.(message, () => {}); } catch {}
};
process.on('error', () => {});
const emit = (kind, extra = {}) => report({ type: 'event', i, name, kind, ...extra });

containWeriftErrors();
await installWebRTC({ engine });
register();
const chess = await import('pigeonchess/bot');
const unsea = await import('unsea');
const { PeerPigeonNode } = await import('./vendor/peerpigeon.js');

// Its key: kept in ~/.wurl/chess-bots/<name>.json, so it is the same player every run.
const keysFile = path.join(os.homedir(), '.wurl', 'chess-bots', `${name.replace(/[^\w.-]+/g, '_')}.json`);
let keys;
try { keys = JSON.parse(fs.readFileSync(keysFile, 'utf8')); } catch {
  keys = await unsea.generateRandomPair();
  fs.mkdirSync(path.dirname(keysFile), { recursive: true });
  fs.writeFileSync(keysFile, JSON.stringify(keys), { mode: 0o600 });
}
const signingKeys = { pub: keys.pub, priv: keys.priv };

// Stockfish 19 lite (the page's own engine), at this bot's level.
const lvl = chess.engineLevel(level);
const require = createRequire(import.meta.url);
let pending = null;
const sf = require('stockfish')('lite-single', (line) => {
  const best = /^bestmove (\S+)/.exec(String(line));
  if (best && pending) { const done = pending; pending = null; done(best[1]); }
});
sf.processCommand('uci');
sf.processCommand(`setoption name Skill Level value ${lvl.skill}`);
const chooseMove = (fen, msLeft) => new Promise((resolve) => {
  // On a clock: never more than a twentieth of what is left.
  const think = Math.max(50, Math.min(lvl.movetime, Number.isFinite(msLeft) ? Math.floor(msLeft / 20) : lvl.movetime));
  const timer = setTimeout(() => { if (pending) { pending = null; resolve(null); } }, think + 5000);
  pending = (uci) => {
    clearTimeout(timer);
    if (!uci || uci === '(none)') return resolve(null);
    resolve({ from: uci.slice(0, 2), to: uci.slice(2, 4), promotion: uci[4] });
  };
  sf.processCommand(`position fen ${fen}`);
  sf.processCommand(`go depth ${lvl.depth} movetime ${think}`);
});

const node = new PeerPigeonNode({
  networkId: network, sessionId: room,
  ...(signalUrl === 'auto' ? {} : { signalingServer: signalUrl }),
  automaticSignalingServer: true,
  minPeers: 2, maxPeers: 10,
  // Every tournament record kept here too, as on a page (pigeonchess utils/tournament.ts KEYS).
  crypto: {}, storage: { replicate: [chess.KEYS] },
});
// The tournaments: in the mesh's own storage (synced with neighbours), as a page keeps them.
const tournamentRoom = async () => (node.storage ? { room: node.storage, signer: keys.pub } : null);

// Sends as a page does: encrypted once the other side's key is known, plain otherwise.
const send = async (peerId, message) => {
  if (node.crypto) {
    try {
      if (!node.getPublicKey(peerId)) await node.waitForPeerKey(peerId, 3000);
      return await node.sendEncryptedDirect(peerId, message, 3000);
    } catch { /* plain, below */ }
  }
  const id = node.sendDirect(peerId, message);
  if (!id) throw new Error(`no route to ${peerId}`);
  return id;
};

let game = null;
let nextSeat = await chess.makeSeatKeys();
const me = { pub: keys.pub, name, keys: unsea.publicKeysOf(keys), bot: { level: lvl.name } };
const short = (key) => `${String(key).slice(0, 6)}…`;
const nameOf = (key) => client.view().flatMap((v) => [...v.names]).find(([k]) => k === key)?.[1] ?? short(key);

const client = new chess.TournamentClient({
  room: tournamentRoom,
  me: () => me,
  sign: (text) => unsea.signMessage(text, keys),
  verify: (text, sig, k) => unsea.verifyMessage(text, sig, k),
  peerId: () => node.getClientId(),
  broadcast: async (message) => node.broadcast(message),
  send,
  isPlaying: () => !!game && !game.finished,
  seat: () => nextSeat.pub,
  onMatched: (match) => {
    const mySeat = nextSeat;
    void chess.makeSeatKeys().then((k) => { nextSeat = k; });
    emit('matched', { text: `plays ${nameOf(match.opponentKey)} as ${match.myColor} (${match.timeControl.minutes}+${match.timeControl.increment})` });
    game = new chess.BotGame({
      gameId: match.gameId, myColor: match.myColor, opponent: match.opponent, mySeat, theirSeat: match.theirSeat ?? '',
      timeControl: match.timeControl, send, chooseMove,
      onEnd: (result, moves, why) => {
        emit('game-over', { text: `${result ?? 'no result'} (${why}, ${moves.length} moves) against ${nameOf(match.opponentKey)}` });
        if (result) void client.report(match.gameId, result, moves);
      },
      log: (line) => emit('note', { text: line }),
    });
    game.start();
  },
});

node.on('message', (m) => {
  if (m?.local) return;
  const from = m.kind === 'broadcast' ? (m.message?.sender ?? m.fromPeerId) : m.fromPeerId;
  let message;
  try { message = typeof m.data === 'string' ? JSON.parse(m.data) : m.data; } catch { return; }
  if (!from || !message) return;
  if (game?.handle(message, from)) return;
  client.handle(message, from);
});

await node.start();
// Its storage writes are signed by its own key (as a signed-in page's are by the account's).
node.storage?.signAs?.(signingKeys);
emit('joined', { text: `on ${network} / ${room} as ${short(keys.pub)} (${lvl.name})` });
client.start();

// Sign up: to every tournament open with room (--join all), or the one named; create one when asked.
const signedUp = new Set();
const tidy = async () => {
  for (const v of client.view()) {
    const id = v.tournament.id;
    const mine = v.signups.some((s) => s.player === me.pub && !s.withdrawn);
    if (mine || signedUp.has(id) || v.status !== 'open') continue;
    if (join !== 'all' && join !== id) continue;
    if (chess.signedUp(v.signups).length >= v.tournament.maxPlayers) continue;
    signedUp.add(id);
    await client.join(id).then(() => emit('signed-up', { text: `signed up for "${v.tournament.name}"` })).catch((e) => { signedUp.delete(id); emit('note', { text: `could not sign up: ${e.message}` }); });
  }
};
if (create) {
  // The storage servers first (a tournament nobody else can read is no use).
  for (let t = 0; t < 60 && !(await tournamentRoom().catch(() => null)); t++) await new Promise((r) => setTimeout(r, 1000));
  await new Promise((r) => setTimeout(r, 3000));
  const id = await client.create({ ...create, play: true }).catch((e) => { emit('note', { text: `could not create: ${e.message}` }); return null; });
  if (id) { signedUp.add(id); emit('created', { text: `created "${create.name}" (${id})` }); }
}
setInterval(() => { void client.refresh().then(tidy); }, 10_000);
// WURL_CHESS_DEBUG: every 15 s, its links and what it knows of the tournaments.
if (process.env.WURL_CHESS_DEBUG) {
  setInterval(() => {
    const v = client.view();
    emit('debug', { text: `links ${node.getConnectedPeers().length}, servers ${node.storageServers ? 'yes' : 'no'}, tournaments ${v.length}: ${v.map((x) => `${x.tournament.name}/${x.status}/${x.signups.length} signed`).join(', ')}` });
  }, 15_000);
}
void client.refresh().then(tidy);

// The standings of its tournaments, now and then (bot 0 only: the others would say the same).
if (i === 0) {
  setInterval(() => {
    const lines = [];
    for (const v of client.view().filter((x) => x.status === 'running' || x.status === 'finished').slice(0, 3)) {
      const played = v.pairings.filter((p) => chess.gameState(p, v.reports).state !== 'unplayed').length;
      lines.push(`"${v.tournament.name}" ${v.status}, ${played}/${v.pairings.length} games`);
      v.table.forEach((r, k) => lines.push(`  ${k + 1}. ${(v.names.get(r.player) ?? short(r.player)).padEnd(22)} ${String(r.points).padStart(4)}  (${r.wins}-${r.draws}-${r.losses})`));
    }
    if (lines.length) report({ type: 'standings', lines });
  }, 60_000);
}

const stop = async () => {
  game?.stop();
  client.stop();
  try { sf.terminate?.(); } catch {}
  try { await node.destroy(); } catch {}
  process.exit(0);
};
process.on('disconnect', stop);
process.on('SIGTERM', stop);
