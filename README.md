# wurl

curl for WebRTC. Point it at a real HTTPS site and it joins as a WebRTC peer,
with no browser, and reports each step the way `curl -v` reports DNS, TLS
and HTTP.

    wurl "https://decentralize.ooo/?network=network:test&room=room-vsmtj78m"

1. Connect to the site's signaling relay (`wss://<host>/ws`).
2. Announce in the network and room from the URL.
3. Discover and list the peers there.
4. Negotiate real WebRTC connections with them: offer, answer, ICE, data channel.
5. Send and receive on the data channels.

Each step is reported with its timing and any failure. Many wurl peers are cheap
processes, so a 50-peer mesh test needs no Docker and no Chromium.

## Scope

It works for sites whose signaling is freertc / PSP: freertc relays,
PeerPigeon and GitPigeon, and anything built on them. Arbitrary WebRTC sites
can't be tested, because each one has its own signaling protocol.

Status: idea. Nothing is built yet.
