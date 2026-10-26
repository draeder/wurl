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
