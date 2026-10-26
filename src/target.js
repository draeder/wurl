// What a wurl URL names: the site's PSP relay, and the network and room to join.
//
//   https://decentralize.ooo/?network=network:test&room=room-1
//     -> relay wss://decentralize.ooo/ws, network network:test, room room-1
//   https://peer.ooo/t/<node id>/?network=...     (a relay behind NAT, via its tunnel)
//     -> relay wss://peer.ooo/t/<node id>/ws
//   wss://decentralize.ooo/ws                      (a relay URL itself)
//
// The page's own parameter names are accepted too: networkName / network_id,
// sessionId / room_id, and signalingServer / signalUrl / ws for the relay.
const DEFAULT_NETWORK = 'network:test';
const DEFAULT_ROOM = 'room:test';

const first = (params, names) => {
  for (const name of names) {
    const value = params.get(name);
    if (value != null && value.trim()) return value.trim();
  }
  return null;
};

// PeerPigeon's example page sets its simulated topology from the URL the same
// way: a ?topology= preset, overridden by ?minPeers=, ?maxPeers= and
// ?tolerantPeers= (or ?tolerant=), clamped to 1-50 / 0-20, min <= max.
const TOPOLOGY_PRESETS = {
  'token-ring': { minPeers: 1, maxPeers: 2, tolerantPeers: 1 },
  star: { minPeers: 1, maxPeers: 20, tolerantPeers: 2 },
  'partial-mesh': { minPeers: 2, maxPeers: 5, tolerantPeers: 1 },
  'dense-mesh': { minPeers: 3, maxPeers: 10, tolerantPeers: 2 },
};
const DEFAULT_TOPOLOGY = 'token-ring';
const PAGE_DEFAULTS = { minPeers: 2, maxPeers: 5, tolerantPeers: 1 };

export function resolveTopology(params, overrides = {}) {
  const asked = String(params.get('topology') || '').trim().toLowerCase();
  let name = TOPOLOGY_PRESETS[asked] || asked === 'custom' ? asked : DEFAULT_TOPOLOGY;
  const number = (value, low, high) => {
    const n = Number(value);
    return value != null && value !== '' && Number.isFinite(n) && n >= low ? Math.min(high, Math.floor(n)) : null;
  };
  const fromUrl = {
    maxPeers: number(params.get('maxPeers'), 1, 50),
    minPeers: number(params.get('minPeers'), 1, 50),
    tolerantPeers: number(params.get('tolerantPeers') ?? params.get('tolerant'), 0, 20),
  };
  const fromFlags = {
    maxPeers: number(overrides.maxPeers, 1, 50),
    minPeers: number(overrides.minPeers, 1, 50),
    tolerantPeers: number(overrides.tolerantPeers, 0, 20),
  };
  // As the page: the preset applies only when the URL gives none of the three;
  // otherwise it starts from the page's own 2 / 5 / 1 and overrides those.
  const anyInUrl = Object.values(fromUrl).some((v) => v !== null);
  const base = anyInUrl ? PAGE_DEFAULTS : (TOPOLOGY_PRESETS[name] ?? TOPOLOGY_PRESETS[DEFAULT_TOPOLOGY]);
  let { minPeers, maxPeers, tolerantPeers } = base;
  let source = asked ? 'URL topology' : 'default topology';
  for (const key of ['maxPeers', 'minPeers', 'tolerantPeers']) {
    if (fromUrl[key] !== null) { ({ maxPeers, minPeers, tolerantPeers } = { maxPeers, minPeers, tolerantPeers, [key]: fromUrl[key] }); source = 'URL'; }
  }
  for (const key of ['maxPeers', 'minPeers', 'tolerantPeers']) {
    if (fromFlags[key] !== null) { ({ maxPeers, minPeers, tolerantPeers } = { maxPeers, minPeers, tolerantPeers, [key]: fromFlags[key] }); source = 'flags'; }
  }
  if (minPeers > maxPeers) minPeers = maxPeers;
  const match = Object.entries(TOPOLOGY_PRESETS).find(([, b]) => b.minPeers === minPeers && b.maxPeers === maxPeers && b.tolerantPeers === tolerantPeers);
  name = match ? match[0] : 'custom';
  return { name, minPeers, maxPeers, tolerantPeers, source };
}

export function resolveTarget(input, overrides = {}) {
  let url;
  try {
    url = new URL(/^[a-z]+:\/\//i.test(input) ? input : `https://${input}`);
  } catch {
    throw new Error(`not a URL: ${input}`);
  }
  const params = url.searchParams;
  let signalUrl = overrides.signal || first(params, ['signalingServer', 'signalUrl', 'ws', 'relay']);
  if (!signalUrl || signalUrl === 'auto') {
    if (url.protocol === 'ws:' || url.protocol === 'wss:') {
      signalUrl = `${url.protocol}//${url.host}${url.pathname.endsWith('/ws') ? url.pathname : '/ws'}`;
    } else if (url.protocol === 'https:' || url.protocol === 'http:') {
      const tunnel = /^\/t\/[0-9a-f]{64}(?=\/|$)/.exec(url.pathname)?.[0] ?? '';
      signalUrl = `${url.protocol === 'https:' ? 'wss' : 'ws'}://${url.host}${tunnel}/ws`;
    } else {
      throw new Error(`unsupported scheme ${url.protocol} (use https://, http://, wss:// or ws://)`);
    }
  }
  return {
    topology: resolveTopology(params, overrides),
    site: url.toString(),
    signalUrl,
    network: overrides.network || first(params, ['network', 'networkName', 'network_id', 'networkId']) || DEFAULT_NETWORK,
    room: overrides.room || first(params, ['room', 'sessionId', 'room_id', 'roomId', 'session_id']) || DEFAULT_ROOM,
  };
}
