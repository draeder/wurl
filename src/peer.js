// One wurl peer: freertc's PSP client with werift underneath, every step
// timed from the moment the peer starts.
import { createSignalingClient } from 'freertc/client';

const PING = 'wurl-ping';
const PONG = 'wurl-pong';

function randomPeerId() {
  return Array.from(crypto.getRandomValues(new Uint8Array(32)), (b) => b.toString(16).padStart(2, '0')).join('');
}

/**
 * Start a peer. `onEvent(event)` gets { at, peer, type, ...details } for each
 * step: status, registered, peers, dial, connected, failed, closed, data, rtt, log.
 * Returns { id, state, stop() }.
 */
export function startPeer({ target, maxConnect = 4, payload = null, pingEveryMs = 2000, verbose = false, onEvent = () => {} }) {
  const id = randomPeerId();
  const started = performance.now();
  const at = () => (performance.now() - started) / 1000;
  const state = {
    id,
    signalingAt: null,
    registeredAt: null,
    seen: new Map(),        // peer id -> first seen (s)
    dialed: new Map(),      // peer id -> dialed at (s)
    connected: new Map(),   // peer id -> connected at (s)
    failed: new Map(),      // peer id -> reason
    received: [],           // { from, bytes, text, at }
    rtt: new Map(),         // peer id -> latest round trip (ms)
    errors: [],
  };
  let stopped = false;
  const emit = (type, details = {}) => { if (!stopped) onEvent({ at: at(), peer: id, type, ...details }); };
  const pings = new Map();
  let client;

  const dial = (peerId) => {
    if (state.dialed.has(peerId) || state.connected.has(peerId)) return;
    if (state.dialed.size >= maxConnect) return;
    state.dialed.set(peerId, at());
    emit('dial', { to: peerId });
    client.initiateConnection(peerId).catch((error) => {
      state.failed.set(peerId, error?.message || String(error));
      emit('failed', { to: peerId, reason: error?.message || String(error) });
    });
  };

  const send = (peerId, text) => {
    try {
      client.sendData(text, peerId);
      return true;
    } catch (error) {
      if (!error?.transient) emit('failed', { to: peerId, reason: `send: ${error.message}` });
      return false;
    }
  };

  // A channel can refuse a frame for a moment right after it opens (SCTP is
  // still settling); retry briefly before calling it a failure.
  const sendWhenReady = (peerId, text, tries = 20) => {
    try {
      client.sendData(text, peerId);
      emit('sent', { to: peerId, bytes: Buffer.byteLength(text) });
    } catch (error) {
      if (tries > 0) setTimeout(() => sendWhenReady(peerId, text, tries - 1), 100);
      else emit('failed', { to: peerId, reason: `send: ${error.message}` });
    }
  };

  client = createSignalingClient({
    peerId: id,
    networkId: target.network,
    roomId: target.room,
    signalUrl: target.signalUrl,
    onLog: verbose ? (message) => emit('log', { message }) : undefined,
    onStatusChange(status) {
      if (status === 'connected' && state.signalingAt === null) state.signalingAt = at();
      if (status === 'error') state.errors.push('signaling error');
      emit('status', { status });
    },
    onRegistered() {
      if (state.registeredAt === null) {
        state.registeredAt = at();
        emit('registered');
      }
      client.requestBootstrap();
    },
    onBootstrap(candidates) {
      const fresh = [];
      for (const candidate of candidates) {
        if (!candidate?.peerId || candidate.peerId === id) continue;
        if (!state.seen.has(candidate.peerId)) {
          state.seen.set(candidate.peerId, at());
          fresh.push(candidate.peerId);
        }
      }
      if (fresh.length) emit('peers', { peers: fresh, total: state.seen.size });
      for (const peerId of state.seen.keys()) dial(peerId);
    },
    onConnectionStateChange({ peerId, state: connection }) {
      if (connection === 'connected' && !state.connected.has(peerId)) {
        state.connected.set(peerId, at());
        state.failed.delete(peerId);
        const since = state.dialed.get(peerId);
        emit('connected', { to: peerId, took: since == null ? null : at() - since, incoming: since == null });
        if (payload != null) sendWhenReady(peerId, payload);
      } else if (connection === 'failed') {
        state.failed.set(peerId, 'connection failed');
        emit('failed', { to: peerId, reason: 'connection failed' });
      } else if (connection === 'closed' && state.connected.has(peerId)) {
        state.connected.delete(peerId);
        emit('closed', { to: peerId });
      }
    },
    onNegotiationFailure(details) {
      const reason = details?.reason || 'negotiation failed';
      if (details?.peerId) state.failed.set(details.peerId, reason);
      emit('failed', { to: details?.peerId ?? null, reason });
    },
    onDataMessage({ peerId, data }) {
      const text = typeof data === 'string' ? data : Buffer.from(data).toString('utf8');
      let message = null;
      try { message = JSON.parse(text); } catch {}
      if (message?.t === PING) {
        send(peerId, JSON.stringify({ t: PONG, n: message.n }));
        return;
      }
      if (message?.t === PONG) {
        const sentAt = pings.get(`${peerId}:${message.n}`);
        if (sentAt != null) {
          const ms = performance.now() - sentAt;
          pings.delete(`${peerId}:${message.n}`);
          state.rtt.set(peerId, ms);
          emit('rtt', { to: peerId, ms });
        }
        return;
      }
      const entry = { from: peerId, bytes: Buffer.byteLength(text), text: text.slice(0, 200), at: at() };
      state.received.push(entry);
      emit('data', entry);
    },
  });

  // Round trips to every connected peer that answers wurl pings (other wurl
  // peers do; browser pages generally don't).
  let n = 0;
  const pinger = setInterval(() => {
    for (const peerId of state.connected.keys()) {
      n += 1;
      pings.set(`${peerId}:${n}`, performance.now());
      send(peerId, JSON.stringify({ t: PING, n }));
    }
  }, pingEveryMs);

  return {
    id,
    state,
    stop() {
      stopped = true;
      clearInterval(pinger);
      try { client.disconnect(); } catch {}
    },
  };
}
