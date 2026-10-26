// WebRTC and WebSocket for Node, in the shape freertc's client expects from a
// browser: werift for RTCPeerConnection and friends, ws for WebSocket.
const WEBRTC_GLOBALS = [
  'RTCCertificate', 'RTCDataChannel', 'RTCDataChannelEvent', 'RTCDtlsTransport', 'RTCError',
  'RTCIceCandidate', 'RTCIceTransport', 'RTCPeerConnection', 'RTCPeerConnectionIceEvent',
  'RTCSctpTransport', 'RTCSessionDescription',
];

let installing;

export function installWebRTC() {
  installing ??= (async () => {
    if (typeof globalThis.WebSocket !== 'function') {
      const { WebSocket } = await import('ws');
      globalThis.WebSocket = WebSocket;
    }
    if (typeof globalThis.RTCPeerConnection !== 'function') {
      const werift = await import('werift');
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

// werift sometimes throws from its own timers when a data channel closes
// mid-flush (seen as "Cannot read properties of undefined (reading 'id')" in
// RTCSctpTransport.dataChannelFlush), which would end the whole process. Errors
// whose stack is inside werift are counted instead; anything else still crashes.
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
