// Lowfly multiplayer relay for Cloudflare Workers + Durable Objects.
//
//   GET /            -> static game (public/index.html), served by the assets binding
//   GET /ws?room=X   -> WebSocket, routed to the Durable Object for room X
//
// Each room is one Durable Object. It does no physics: every client flies its own plane and sends its state,
// and the room forwards each message to everyone else, stamped with the sender's id (field "f").
// Uses WebSocket hibernation so an idle room costs nothing.

import { DurableObject } from 'cloudflare:workers';

const MAX_PLAYERS = 8;
const MAX_MSG = 2048;                              // bytes; real state packets are ~250
const RELAY = new Set(['s', 'dead', 'kill']);      // message types that are forwarded to the other players

const cleanRoom = (s) => (String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 12)) || 'OPEN';
const cleanName = (s) => (String(s || '').replace(/[^\w \-.]/g, '').trim().slice(0, 16)) || 'Pilot';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') return new Response('Expected a WebSocket connection', { status: 426 });
      const room = cleanRoom(url.searchParams.get('room'));
      return env.ROOMS.get(env.ROOMS.idFromName(room)).fetch(request);
    }
    return env.ASSETS.fetch(request);
  },
};

export class Room extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    const pair = new WebSocketPair();
    const client = pair[0], server = pair[1];
    this.ctx.acceptWebSocket(server);

    const others = this.ctx.getWebSockets().filter((w) => w !== server);
    if (others.length >= MAX_PLAYERS) {
      server.send(JSON.stringify({ t: 'error', msg: 'Room is full (' + MAX_PLAYERS + ' players).' }));
      server.close(1008, 'Room full');
      return new Response(null, { status: 101, webSocket: client });
    }

    const used = new Set(others.map((w) => (w.deserializeAttachment() || {}).slot));
    let slot = 0; while (used.has(slot)) slot++;
    const me = { id: crypto.randomUUID().slice(0, 6), slot, name: cleanName(url.searchParams.get('name')) };
    server.serializeAttachment(me);

    server.send(JSON.stringify({
      t: 'hello', id: me.id, slot: me.slot, now: Date.now(),
      players: others.map((w) => w.deserializeAttachment()).filter(Boolean),
    }));
    this.broadcast(server, JSON.stringify({ t: 'join', id: me.id, slot: me.slot, name: me.name }));
    return new Response(null, { status: 101, webSocket: client });
  }

  broadcast(from, text) {
    for (const w of this.ctx.getWebSockets()) {
      if (w === from) continue;
      try { w.send(text); } catch (e) { /* socket is closing */ }
    }
  }

  webSocketMessage(ws, data) {
    if (typeof data !== 'string' || data.length > MAX_MSG) return;
    const me = ws.deserializeAttachment();
    if (!me) return;
    let m;
    try { m = JSON.parse(data); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'ping') {                                  // clock sync: the client works out its offset from server time
      ws.send(JSON.stringify({ t: 'pong', c: m.c, s: Date.now() }));
      return;
    }
    if (!RELAY.has(m.t)) return;
    m.f = me.id;                                           // the sender cannot spoof who it is
    this.broadcast(ws, JSON.stringify(m));
  }

  webSocketClose(ws, code, reason, wasClean) { this.drop(ws, code); }
  webSocketError(ws) { this.drop(ws, 1011); }

  drop(ws, code) {
    const me = ws.deserializeAttachment();
    try { ws.close(code && code !== 1005 && code !== 1006 ? code : 1000, 'bye'); } catch (e) { /* already closed */ }
    if (me) this.broadcast(ws, JSON.stringify({ t: 'leave', id: me.id }));
  }
}
