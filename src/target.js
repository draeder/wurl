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
    site: url.toString(),
    signalUrl,
    network: overrides.network || first(params, ['network', 'networkName', 'network_id', 'networkId']) || DEFAULT_NETWORK,
    room: overrides.room || first(params, ['room', 'sessionId', 'room_id', 'roomId', 'session_id']) || DEFAULT_ROOM,
  };
}
