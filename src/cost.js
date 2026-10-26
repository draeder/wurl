// wurl --cost: what a run cost the relays, as Cloudflare would bill it.
// Reads GET /debug/cost from each Node relay (freertc's scripts/node-relay.mjs
// counts the Worker requests, Durable Object requests and messages, and D1
// queries the worker code would cause) before and after the run.

const FIELDS = ['workerRequests', 'doRequests', 'doWsMessages', 'd1Reads', 'd1Writes', 'd1RowsWritten'];

function httpBase(url) {
  const parsed = new URL(url.replace(/^ws/, 'http'));
  return `${parsed.protocol}//${parsed.host}`;
}

export async function readCost(urls) {
  const out = new Map();
  await Promise.all(urls.map(async (url) => {
    const base = httpBase(url);
    try {
      const response = await fetch(`${base}/debug/cost`);
      const body = await response.json();
      out.set(base, { name: body.name ?? base, cost: body.cost });
    } catch (error) {
      out.set(base, { name: base, error: error.message });
    }
  }));
  return out;
}

export function costDelta(before, after) {
  const rows = [];
  for (const [base, end] of after) {
    const start = before.get(base);
    if (end.error || !start || start.error) {
      rows.push({ relay: end.name, error: end.error ?? start?.error ?? 'no starting count' });
      continue;
    }
    const row = { relay: end.name };
    for (const field of FIELDS) row[field] = (end.cost[field] ?? 0) - (start.cost[field] ?? 0);
    const paths = {};
    for (const [key, value] of Object.entries(end.cost.byPath ?? {})) {
      const d = value - (start.cost.byPath?.[key] ?? 0);
      if (d) paths[key] = d;
    }
    row.byPath = paths;
    rows.push(row);
  }
  return rows;
}

/** Cloudflare's billed DO requests: requests plus incoming WebSocket messages at 20:1. */
export const billedDo = (row) => row.doRequests + row.doWsMessages / 20;

export function formatCost(rows, { peers, links }) {
  const lines = [];
  const total = Object.fromEntries(FIELDS.map((f) => [f, 0]));
  const pad = (value, width) => String(value).padStart(width);
  lines.push(`cost       relay              worker req   DO req   DO WS msgs   DO billed   D1 reads   D1 writes`);
  for (const row of rows) {
    if (row.error) { lines.push(`           ${row.relay.padEnd(18)} could not read /debug/cost: ${row.error}`); continue; }
    for (const f of FIELDS) total[f] += row[f];
    lines.push(`           ${String(row.relay).slice(0, 18).padEnd(18)} ${pad(row.workerRequests, 10)} ${pad(row.doRequests, 8)} ${pad(row.doWsMessages, 12)} ${pad(billedDo(row).toFixed(1), 11)} ${pad(row.d1Reads, 10)} ${pad(row.d1Writes, 11)}`);
  }
  lines.push(`           ${'total'.padEnd(18)} ${pad(total.workerRequests, 10)} ${pad(total.doRequests, 8)} ${pad(total.doWsMessages, 12)} ${pad(billedDo(total).toFixed(1), 11)} ${pad(total.d1Reads, 10)} ${pad(total.d1Writes, 11)}`);
  const per = (value, n) => (n ? (value / n).toFixed(1) : '-');
  lines.push(`           per peer: ${per(total.workerRequests, peers)} worker req, ${per(billedDo(total), peers)} DO billed, ${per(total.d1Reads + total.d1Writes, peers)} D1 queries`
    + `; per link (${links}): ${per(total.workerRequests, links)} worker req, ${per(billedDo(total), links)} DO billed, ${per(total.d1Reads + total.d1Writes, links)} D1 queries`);
  return lines;
}
