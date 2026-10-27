# wurl

curl for WebRTC. Point it at a real HTTPS site that speaks PSP (freertc's
signaling standard) and it joins as a WebRTC peer, with no browser. It reports
each step the way `curl -v` reports DNS, TLS and HTTP.

A run against a local relay with one other peer in the room:

```
$ wurl -d "hello from wurl" "http://127.0.0.1:18800/?network=network:test&room=wurl-pair2"
* wurl http://127.0.0.1:18800/?network=network:test&room=wurl-pair2
* relay ws://127.0.0.1:18800/ws, network network:test, room wurl-pair2
* [0.00s] connecting to the relay
* [0.02s] relay connected
* [0.02s] registered and announced in network:test / wurl-pair2
* [0.02s] found 1 peer(s): 42317db2 (1 so far)
> [0.02s] offer to 42317db2
* [0.42s] data channel open with 42317db2 after 0.40s
> [0.52s] sent 15 bytes to 42317db2

relay      ws://127.0.0.1:18800/ws connected 0.02s
registered 0.02s in network:test / wurl-pair2
peers      1 found, 1 connected, 0 failed
  42317db2  open in 0.40s, rtt 4 ms
```

The steps:

1. Connect to the site's PSP relay (`wss://<host>/ws`).
2. Register and announce in the network and room from the URL.
3. Discover the peers there.
4. Negotiate real WebRTC connections with them (offer, answer, ICE, data channel).
5. Exchange data: round trips to other wurl peers, plus anything you send with `-d`.

The PSP client is freertc's own (`freertc/client`), so wurl does exactly what
freertc browsers do. WebRTC comes from werift.

## Install

```
npm install
npm link        # optional: puts `wurl` on your PATH
```

Needs Node 20 or later.

## Use

```
wurl [options] <url>
```

`<url>` is a PSP site such as `https://decentralize.ooo/?network=network:test&room=room-1`,
or a relay URL (`wss://host/ws`). The network and room come from `?network=` and
`?room=` (the pages' `networkName` and `sessionId` work too), or default to
`network:test` and `room:test`. A relay behind NAT, reached through its tunnel
(`https://peer.ooo/t/<node id>/...`), works too.

| Option | |
| --- | --- |
| `-n, --network <name>` | network to join (overrides the URL) |
| `-r, --room <id>` | room to join (overrides the URL) |
| `--signal <url>` | relay to use instead of the one the URL implies |
| `-p, --peers <n>` | run n peers in one process; they connect to each other and to everyone else in the room |
| `-c, --connect <n>` | connections each peer dials, at most (default 4) |
| `-t, --time <s>` | longest run (default 20 s). A single peer stops sooner, once every peer it found has connected or failed |
| `-d, --data <text>` | send this on each data channel when it opens |
| `-v, --verbose` | also show freertc's own signaling and WebRTC log |
| `-s, --silent` | only the summary |
| `--json` | the summary as JSON |
| `--public-load` | allow more than 5 peers against a public relay |

Exit status, curl-style: `0` every peer registered and connected to at least
one other; `6` the relay couldn't be reached; `7` signaling failed before
registering; `52` registered, but nobody else is in the room; `28` peers
were found but none connected.

## A mesh test without a browser

Run big tests against a local relay: every peer's signaling counts against a
public relay's daily request limit, and a 50-peer test once used up the free
plan's 100,000 requests in an hour. Start a relay (`node scripts/node-relay.mjs`
in freertc), then:

```
wurl -p 50 -t 60 --signal ws://127.0.0.1:8788/ws "https://decentralize.ooo/?network=network:test&room=room-vsmtj78m"
```

wurl refuses more than 5 peers against a public relay unless you add `--public-load`.

This runs 50 peers in one process. Locally, 20 wurl peers formed one mesh using
169 MB in total, about 8 MB a peer. The summary reports links per peer, whether
the wurl peers form one group, and the median data-channel open time and
round trip.

## Broadcast and direct messages (`--messages`)

```
wurl --messages -p 50 -t 120 --signal ws://127.0.0.1:8788/ws "https://decentralize.ooo/?network=network:test&room=wurl-50&topology=partial-mesh"
```

`-m, --messages` runs the peers as PeerPigeon nodes, using PeerPigeon's own code
bundled in `src/vendor/` (refresh it from `../PeerPigeon` with `npm run vendor:peerpigeon`).
Every peer:

- broadcasts once, and gossip should carry each broadcast to every other peer;
- sends one direct message to the peer halfway round, usually several hops away,
  so the mesh has to route it.

Each peer runs in its own Node process, as a real peer runs on its own machine.
The wurl process only coordinates: it starts each peer, follows the mesh from what
the peers report, and tells them all to go once the mesh has settled.

WebRTC is native by default: node-datachannel (libdatachannel, in C++), when it
is installed, which it is with `npm install` (an optional dependency with
prebuilt binaries). Without it, peers fall back to werift, which does DTLS and
SCTP in JavaScript. `--webrtc native|werift|auto` chooses. On a 2-CPU machine,
with 20 peers, the broadcast median was 9 ms native and 61 ms with werift.

The test is asynchronous, as P2P is. Each peer joins at its own random time
within `--join-window` seconds (default 20). No one sends anything until the
mesh is formed and settled: every peer has joined, the peers are one group,
each has at least `minPeers` links, and that has held for `--settle` seconds
(default 3). Then
each peer sends its broadcast and its direct message at its own random times,
each up to `--send-spread` seconds (default 10) after the last, while links are
still settling. Each latency is measured from that message's own send time, and
the report says when the last peer joined, when the last one got a link, when the mesh settled, and how many messages went out before
the mesh had fully formed.

`--phased` runs the old lockstep test instead: everyone joins at once, one mesh
must hold steady for `--settle` seconds, then all broadcasts go out, then all
direct messages.

The report gives the mesh shape (groups, links per peer), the broadcast and
direct delivery counts with latency (median, p95, max), and names every peer
and message that missed. The exit status is 0 only when there is one mesh and
everything arrived.

The topology comes from the URL, exactly as on PeerPigeon's example page:
`?topology=token-ring|star|partial-mesh|dense-mesh` picks a preset (token-ring is
the default: minPeers 1, maxPeers 2; partial-mesh is 2 / 5, dense-mesh 3 / 10, star
1 / 20), and `?minPeers=`, `?maxPeers=` and `?tolerantPeers=` set them directly (then
the preset doesn't apply, and the page's 2 / 5 / 1 is the starting point). The
report shows the topology used, each peer's own link count against maxPeers, and
the mesh counted by links either end lists, which differs when links are one-sided.

On a local relay, 30 peers with `topology=partial-mesh` and a 15 s settle: every peer
kept 4–5 links, broadcast 870/870, and direct 16/30, failing in both directions
between the same pairs.

| Option | |
| --- | --- |
| `--min-links <n>` | override minPeers |
| `--max-links <n>` | override maxPeers |
| `--tolerant <n>` | override tolerantPeers |
| `--join-window <s>` | peers join at random times within this (default 20) |
| `--send-spread <s>` | each send waits a random 0..s after the last (default 10) |
| `--phased` | the lockstep test: all join at once, the mesh holds for `--settle`, then all broadcasts, then all direct |
| `--settle <s>` | how long the formed mesh must hold steady before anyone sends (default 3) |
| `--webrtc <engine>` | `native` (node-datachannel), `werift`, or `auto` (native when installed; default) |
| `--wait <s>` | how long to wait for messages to arrive (default 20) |

## Tests

```
npm test
```

They start a local freertc relay (from the freertc package) and run wurl
against it.
