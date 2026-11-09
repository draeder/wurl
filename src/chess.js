// wurl --chess: computer players for PigeonChess tournaments. Each bot runs in its own Node process
// (src/chess-bot.js) with a key of its own, signs up for tournaments, and plays its games with Stockfish
// at its level, against people on the page or other bots. This process starts them and prints what
// they do, until Ctrl-C.
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const LEVELS = ['easy', 'medium', 'hard', 'master'];

/**
 * bots: how many; level: one of LEVELS, or 'mixed' (each a different one in turn); join: 'all' (every
 * open tournament with room) or a tournament id; create: { name, players, clock } for bot 0 to create one.
 */
export function runChessBots({ bots, level, join, create, prefix, signalUrls, network, room, engine, say }) {
  const children = [];
  const started = Date.now();
  const stamp = () => {
    const s = Math.round((Date.now() - started) / 1000);
    return `${String(Math.floor(s / 3600)).padStart(2, '0')}:${String(Math.floor(s / 60) % 60).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
  };
  for (let i = 0; i < bots; i++) {
    const lvl = level === 'mixed' ? LEVELS[i % LEVELS.length] : level;
    const name = `${prefix} ${i + 1} (${lvl})`;
    const config = {
      // --create: every bot is in that one tournament (and stops when it is over).
      i, name, level: lvl, network, room, join: create ? create.name : join, engine,
      signalUrl: signalUrls[i % signalUrls.length] ?? 'auto',
      create: i === 0 && create ? create : null,
    };
    const child = fork(fileURLToPath(new URL('./chess-bot.js', import.meta.url)), [], {
      env: { ...process.env, WURL_BOT: JSON.stringify(config) },
      stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
    });
    let errors = '';
    child.stderr.on('data', (d) => { errors = (errors + d).slice(-2000); });
    child.on('message', (m) => {
      if (m?.type === 'event') say(`* [${stamp()}] ${m.name}: ${m.text ?? m.kind}`);
      else if (m?.type === 'standings') { say(`* [${stamp()}] standings`); for (const line of m.lines) say(`    ${line}`); }
    });
    child.on('exit', (code) => { if (code) say(`* [${stamp()}] ${name} stopped (exit ${code})${errors ? `:\n${errors.trim().split('\n').slice(-6).join('\n')}` : ''}`); });
    children.push(child);
  }
  const stop = () => { for (const c of children) { try { c.disconnect(); } catch {} try { c.kill('SIGTERM'); } catch {} } };
  return { stop, done: new Promise((resolve) => { let left = children.length; for (const c of children) c.on('exit', () => { if (--left === 0) resolve(); }); }) };
}

/** "3+2" → { minutes: 3, increment: 2 }; "0" or "none" → no clock. */
export function parseClock(text) {
  if (!text || text === 'none' || text === '0') return { minutes: 0, increment: 0 };
  const m = /^(\d+(?:\.\d+)?)(?:\+(\d+))?$/.exec(String(text).trim());
  if (!m) throw new Error(`--clock takes minutes+increment, like 3+2 (got "${text}")`);
  return { minutes: Number(m[1]), increment: Number(m[2] ?? 0) };
}

export { LEVELS };
