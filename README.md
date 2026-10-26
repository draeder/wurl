# wurl

curl over WebRTC: make HTTP requests to a peer, not a host, over a WebRTC data
channel. No server needs a public address or an open port.

    wurl wrtc://<network>/<room>/<peer-id>/path/to/resource

Signaling (finding the peer and exchanging the offer, answer and ICE
candidates) goes through freertc relays. After that the request and response
travel peer to peer over the data channel, hole-punched through NAT.

Status: idea. Nothing is built yet.
