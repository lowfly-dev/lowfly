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
const RELAY = new Set(['s', 'dead', 'kill', 'f', 'm', 'hit', 'fr']);   // message types that are forwarded to the other players
const TARGETED = new Set(['hit', 'fr']);          // these go only to the pilot named in "to" (damage, friend requests)
const CHAT_KEEP = 60;                              // chat messages the room remembers (saved in Durable Object storage, so they survive restarts)
const CHAT_LEN = 120;
const ROOM = 'SKY';                                // one room for everyone

const cleanName = (s) => (String(s || '').replace(/[^\w \-.]/g, '').trim().slice(0, 16)) || 'Pilot';

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') return new Response('Expected a WebSocket connection', { status: 426 });
      return env.ROOMS.get(env.ROOMS.idFromName(ROOM)).fetch(request);
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

    const chat = (await this.ctx.storage.get('chat')) || [];
    server.send(JSON.stringify({
      t: 'hello', id: me.id, slot: me.slot, now: Date.now(),
      players: others.map((w) => w.deserializeAttachment()).filter(Boolean),
      chat,
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

  async webSocketMessage(ws, data) {
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
    if (m.t === 'chat') {                                  // saved chat: stored, then sent to everyone including the sender
      const x = String(m.x || '').replace(/[\u0000-\u001f\u007f]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, CHAT_LEN);
      const now = Date.now();
      if (!x || now - (me.lastChat || 0) < 400) return;    // empty or too fast
      me.lastChat = now; ws.serializeAttachment(me);
      const msg = { n: me.name, s: me.slot, x, ts: now };
      const hist = (await this.ctx.storage.get('chat')) || [];
      hist.push(msg);
      await this.ctx.storage.put('chat', hist.slice(-CHAT_KEEP));
      const text = JSON.stringify({ t: 'chat', ...msg });
      for (const w of this.ctx.getWebSockets()) { try { w.send(text); } catch (e) { /* closing */ } }
      return;
    }
    if (!RELAY.has(m.t)) return;
    m.f = me.id;                                           // the sender cannot spoof who it is
    if (TARGETED.has(m.t)) {
      if (typeof m.to !== 'string') return;
      if (m.t === 'fr' && m.k !== 'req' && m.k !== 'acc') return;
      const text = JSON.stringify(m);
      for (const w of this.ctx.getWebSockets()) {
        const a = w.deserializeAttachment();
        if (w !== ws && a && a.id === m.to) { try { w.send(text); } catch (e) { /* closing */ } break; }
      }
      return;
    }
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
