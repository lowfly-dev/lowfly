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

// ---- 2v2 duel settings (the client in index.html expects exactly this protocol) ----
const TKEYS = ['RUS', 'USA'];
const SEATS = 2;                                   // pilots per team
const FORM_MS = 10000;                             // countdown once two pilots are queued
const MATCH_MS = 5 * 60 * 1000;                    // 5 minute match
const OVER_MS = 10000;                             // how long the result stays up
const PLANE_OF = { RUS: [3], USA: [4, 5] };        // indexes into the client's PLANES list: Su-35 / F/A-18, F-16

const OWNER_CODE = '098';                          // put this in your pilot name and chat shows an OWNER badge

const cleanName = (s) => (String(s || '').replace(/[^\w \-.]/g, '').trim().slice(0, 16)) || 'Pilot';
// "Ben098" -> shown as "Ben" with the owner flag set
const parseName = (raw) => {
  const c = String(raw || '').replace(/[^\w \-.]/g, '');
  const own = c.indexOf(OWNER_CODE) >= 0;
  return { name: cleanName(own ? c.split(OWNER_CODE).join('') : c), own };
};

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
    const pn = parseName(url.searchParams.get('name'));
    const me = { id: crypto.randomUUID().slice(0, 6), slot, name: pn.name, own: pn.own ? 1 : 0 };
    server.serializeAttachment(me);

    const chat = (await this.ctx.storage.get('chat')) || [];
    await this.duelLoad();
    this.duelPrune(me.id);
    server.send(JSON.stringify({
      t: 'hello', id: me.id, slot: me.slot, now: Date.now(),
      players: others.map((w) => w.deserializeAttachment()).filter(Boolean),
      chat,
      duel: this.duelSnap(),
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
      if (me.own) msg.o = 1;
      const hist = (await this.ctx.storage.get('chat')) || [];
      hist.push(msg);
      await this.ctx.storage.put('chat', hist.slice(-CHAT_KEEP));
      const text = JSON.stringify({ t: 'chat', ...msg });
      for (const w of this.ctx.getWebSockets()) { try { w.send(text); } catch (e) { /* closing */ } }
      return;
    }
    if (m.t === 'duel') { await this.duelMsg(me, m); return; }
    if (!RELAY.has(m.t)) return;
    m.f = me.id;                                           // the sender cannot spoof who it is
    if (m.t === 'dead') { await this.duelDead(me.id, typeof m.by === 'string' ? m.by : ''); }
    if (TARGETED.has(m.t)) {
      if (typeof m.to !== 'string') return;
      if (m.t === 'hit' && !(await this.duelCanHit(me.id, m.to))) return;   // no friendly fire, no hits outside the match
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
    if (me) {
      this.broadcast(ws, JSON.stringify({ t: 'leave', id: me.id }));
      this.duelDrop(me.id, ws);
    }
  }

  // ================= 2v2 duel =================
  // State: ph = idle | forming | live | over. teams/planes are keyed by pilot id. t1 = start time (forming), end time (live).
  // Phase changes run off a Durable Object alarm, and the state is saved so a restart does not lose a match.

  async duelLoad() {
    if (this.duel) return;
    this.duel = (await this.ctx.storage.get('duel')) || { ph: 'idle', no: 0, teams: {}, planes: {}, score: { RUS: 0, USA: 0 }, queue: [], t1: 0, win: '', why: '' };
  }
  async duelSave() { await this.ctx.storage.put('duel', this.duel); }

  duelSnap() {
    const d = this.duel;
    if (!d) return { t: 'duel', ph: 'idle', no: 0, teams: {}, planes: {}, score: { RUS: 0, USA: 0 }, queue: [], t1: 0 };
    const o = { t: 'duel', ph: d.ph, no: d.no, teams: d.teams, planes: d.planes, score: d.score, queue: d.queue, t1: d.t1 };
    if (d.ph === 'over') { o.win = d.win; o.why = d.why; }
    return o;
  }
  duelBroadcast() { const text = JSON.stringify(this.duelSnap()); for (const w of this.ctx.getWebSockets()) { try { w.send(text); } catch (e) { /* closing */ } } }

  duelConnected(skipWs) {
    const ids = new Set();
    for (const w of this.ctx.getWebSockets()) { if (w === skipWs) continue; const a = w.deserializeAttachment(); if (a) ids.add(a.id); }
    return ids;
  }
  // forget anyone who is no longer connected (after a restart, or a socket that died without a clean close)
  duelPrune(extraId, skipWs) {
    const d = this.duel; if (!d) return false;
    const live = this.duelConnected(skipWs); if (extraId) live.add(extraId);
    let changed = false;
    const q = d.queue.filter((id) => live.has(id)); if (q.length !== d.queue.length) { d.queue = q; changed = true; }
    for (const id of Object.keys(d.teams)) if (!live.has(id)) { delete d.teams[id]; delete d.planes[id]; changed = true; }
    return changed;
  }
  teamCount(k) { return Object.keys(this.duel.teams).filter((id) => this.duel.teams[id] === k).length; }
  freePlane(k) {
    const used = new Set(Object.keys(this.duel.teams).filter((id) => this.duel.teams[id] === k).map((id) => this.duel.planes[id]));
    return PLANE_OF[k].find((p) => !used.has(p)) ?? PLANE_OF[k][0];
  }
  seat(id, k) { this.duel.teams[id] = k; this.duel.planes[id] = this.freePlane(k); }

  async duelMsg(me, m) {
    await this.duelLoad(); const d = this.duel;
    this.duelPrune();
    if (m.k === 'q') {
      if (d.queue.indexOf(me.id) < 0 && !d.teams[me.id]) d.queue.push(me.id);
      this.duelAdvance();
    } else if (m.k === 'x') {
      d.queue = d.queue.filter((id) => id !== me.id);
      if (d.teams[me.id]) { delete d.teams[me.id]; delete d.planes[me.id]; }
      this.duelAdvance(); this.duelCheckForfeit();
    } else return;
    await this.duelSave(); this.duelBroadcast(); await this.duelAlarm();
  }

  // move the state machine forward after a queue/team change (no timers fire here)
  duelAdvance() {
    const d = this.duel;
    if (d.ph === 'idle' && d.queue.length >= 2) { d.ph = 'forming'; d.t1 = Date.now() + FORM_MS; }
    else if (d.ph === 'forming' && d.queue.length < 2) { d.ph = 'idle'; d.t1 = 0; }
    else if (d.ph === 'live') {                            // queued pilots take open seats, smaller team first
      while (d.queue.length) {
        const r = this.teamCount('RUS'), u = this.teamCount('USA');
        if (r >= SEATS && u >= SEATS) break;
        const k = (r < SEATS && (r <= u || u >= SEATS)) ? 'RUS' : 'USA';
        this.seat(d.queue.shift(), k);
      }
    }
  }
  duelCheckForfeit() {
    const d = this.duel; if (d.ph !== 'live') return;
    const r = this.teamCount('RUS'), u = this.teamCount('USA');
    if (r && u) return;
    if (!r && !u) { this.duelReset(); return; }
    d.ph = 'over'; d.win = r ? 'RUS' : 'USA'; d.why = 'forfeit'; d.t1 = Date.now() + OVER_MS;
  }
  duelReset() { const d = this.duel; d.ph = 'idle'; d.teams = {}; d.planes = {}; d.score = { RUS: 0, USA: 0 }; d.t1 = 0; d.win = ''; d.why = ''; }

  async duelAlarm() {                                       // schedule the next phase change (or clear it)
    const d = this.duel;
    if (d.ph === 'idle' || !d.t1) { await this.ctx.storage.deleteAlarm(); return; }
    await this.ctx.storage.setAlarm(Math.max(d.t1, Date.now() + 50));
  }
  async alarm() {
    await this.duelLoad(); const d = this.duel;
    this.duelPrune();
    if (d.ph === 'forming') {
      if (d.queue.length < 2) { d.ph = 'idle'; d.t1 = 0; }
      else {
        const take = d.queue.splice(0, SEATS * 2);          // up to 4 pilots, alternate teams: RUS, USA, RUS, USA
        d.no++; d.teams = {}; d.planes = {}; d.score = { RUS: 0, USA: 0 }; d.win = ''; d.why = '';
        take.forEach((id, i) => this.seat(id, TKEYS[i % 2]));
        d.ph = 'live'; d.t1 = Date.now() + MATCH_MS;
      }
    } else if (d.ph === 'live') {
      const a = d.score.RUS, b = d.score.USA;
      d.ph = 'over'; d.win = a > b ? 'RUS' : b > a ? 'USA' : 'draw'; d.why = 'time'; d.t1 = Date.now() + OVER_MS;
    } else if (d.ph === 'over') {
      this.duelReset();
      if (d.queue.length >= 2) { d.ph = 'forming'; d.t1 = Date.now() + FORM_MS; }
    }
    await this.duelSave(); this.duelBroadcast(); await this.duelAlarm();
  }

  async duelDead(victimId, killerId) {                      // a pilot crashed: if an enemy shot them during a live match, that team scores
    await this.duelLoad(); const d = this.duel;
    if (d.ph !== 'live') return;
    const vt = d.teams[victimId], kt = d.teams[killerId];
    if (!vt || !kt || vt === kt || killerId === victimId) return;
    d.score[kt]++;
    await this.duelSave(); this.duelBroadcast();
  }

  // same rule the client uses: both free-flying = ok; otherwise both in the match, live, on opposite teams
  async duelCanHit(fromId, toId) {
    await this.duelLoad(); const d = this.duel;
    const a = d.teams[fromId] || null, b = d.teams[toId] || null;
    if (!a && !b) return true;
    return !!a && !!b && d.ph === 'live' && a !== b;
  }

  duelDrop(id, ws) {                                        // a pilot disconnected
    if (!this.duel) { this.duelLoad().then(() => this.duelDrop(id, ws)); return; }
    const d = this.duel;
    d.queue = d.queue.filter((x) => x !== id);
    delete d.teams[id]; delete d.planes[id];
    this.duelPrune(null, ws);
    this.duelAdvance(); this.duelCheckForfeit();
    this.duelSave().then(() => { this.duelBroadcast(); return this.duelAlarm(); });
  }
}
