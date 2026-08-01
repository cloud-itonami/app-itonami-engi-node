# engi-node

The inbound half of an [`engi`](https://github.com/kotoba-lang/engi) peer,
deployed where nothing can dial out: a Cloudflare Worker.

**Live:** `https://engi-node.04-feasts-minded.workers.dev`

```
GET /head   what it is, who is connected, what it last saw
GET /peer   WebSocket upgrade — the peer connection itself
```

## It is not a validator, and says so

It runs no pacemaker and casts no votes. It accepts connections, decodes
messages with the same `engi.wire` a validator uses, and reports what it saw.
That is enough to answer the question it exists to answer — **does the
transport work in a Worker** — without pretending to a consensus role nothing
here is ready for. `/head` states it in its own response, because a service
that stays quiet about what it is will be assumed to be the other thing.

## Why a Worker needs the accepting half

`engi.net.ws` dials out. A Durable Object cannot hold an outbound WebSocket
the way a Node process can, so a mesh containing a Worker only works if the
Worker is the thing being called. `engi.net.server` is that half, and it runs
the **same** policy — same bounded queue, same strike accounting — so a
hostile peer costs the same whether it called us or we called it.

## Measured, not assumed

An nbb client dialling the deployed Worker over `wss`, through a real network
and a real TLS terminator, with the hub queried **while the socket was open**:

```
live after dial : [:hub]
client saw      : [[:hub :vote]]
hub /head       : {"peers":["peer-1"],"peer-count":1,
                   "seen":[{"peer":"peer-1","type":"new-view"}]}
```

Reproduce with `engi`'s `script/check-worker.cljs`.

## A Durable Object forgets

Querying `/head` **after** disconnecting shows an empty peer list and an empty
log: with no open socket and no pending request the DO can be evicted, taking
its in-memory record with it. That is correct behaviour and it is why the
check queries while connected — asking afterwards measures eviction, not the
transport.

For a real validator it also means peer state and anything derived from it
must be persisted or rebuilt on wake. This node deliberately does neither,
because it holds nothing worth keeping.

## Build

```bash
npm install && npx shadow-cljs release worker && npx wrangler deploy
```
