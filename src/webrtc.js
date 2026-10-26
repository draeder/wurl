// WebRTC and WebSocket for Node, in the shape freertc's client expects from a
// browser: werift for RTCPeerConnection and friends, ws for WebSocket.
const WEBRTC_GLOBALS = [
  'RTCCertificate', 'RTCDataChannel', 'RTCDataChannelEvent', 'RTCDtlsTransport', 'RTCError',
  'RTCIceCandidate', 'RTCIceTransport', 'RTCPeerConnection', 'RTCPeerConnectionIceEvent',
  'RTCSctpTransport', 'RTCSessionDescription',
];

let installing;

/**
 * werift 0.24 bug: on DATA_CHANNEL_OPEN it creates the channel, hands it to the
 * app, then queues the ACK by looking the channel up again by stream id. If
 * the app closed it in between (freertc closes the duplicate channel at once
 * when two peers dial each other: "keeping the local one"), the lookup is
 * undefined, the ACK is queued for no channel, and dataChannelFlush throws
 * "Cannot read properties of undefined (reading 'id')" from a timer, which
 * ends the process. Drop queued items whose channel is gone before flushing.
 */
function patchWeriftClosedChannelAck(werift) {
  const proto = werift.RTCSctpTransport?.prototype;
  if (!proto || proto.__wurlClosedChannelAckPatch || typeof proto.dataChannelFlush !== 'function') return;
  const flush = proto.dataChannelFlush;
  proto.dataChannelFlush = function dataChannelFlush(...args) {
    if (Array.isArray(this.dataChannelQueue) && this.dataChannelQueue.some((item) => !item?.[0])) {
      this.dataChannelQueue = this.dataChannelQueue.filter((item) => item?.[0]);
    }
    return flush.apply(this, args);
  };
  proto.__wurlClosedChannelAckPatch = true;
}

export function installWebRTC() {
  installing ??= (async () => {
    if (typeof globalThis.WebSocket !== 'function') {
      const { WebSocket } = await import('ws');
      globalThis.WebSocket = WebSocket;
    }
    if (typeof globalThis.RTCPeerConnection !== 'function') {
      const werift = await import('werift');
      patchWeriftClosedChannelAck(werift);
      for (const name of WEBRTC_GLOBALS) {
        if (globalThis[name] === undefined && werift[name] !== undefined) globalThis[name] = werift[name];
      }
      const Base = werift.RTCPeerConnection;
      globalThis.RTCPeerConnection = class WurlPeerConnection extends Base {
        constructor(configuration = {}) {
          super({ ...configuration, iceCandidatePoolSize: 0 });
        }
      };
    }
  })();
  return installing;
}

// A werift error thrown from its own timers would end the whole process. The
// known one is fixed above (patchWeriftClosedChannelAck); any other error whose
// stack is inside werift is counted and reported instead. Anything else still
// crashes.
export const weriftErrors = { count: 0, last: null };

export function containWeriftErrors() {
  const handle = (error) => {
    const stack = String(error?.stack || '');
    if (/[\\/]werift[\\/]/.test(stack)) {
      weriftErrors.count += 1;
      weriftErrors.last = String(error?.message || error);
      return;
    }
    console.error(error);
    process.exit(70);
  };
  process.on('uncaughtException', handle);
  process.on('unhandledRejection', handle);
}
