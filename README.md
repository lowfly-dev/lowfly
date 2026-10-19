# Lowfly multiplayer (Cloudflare Workers + Durable Objects)

```
public/index.html   the game (single-player still works; multiplayer is in the menu)
src/worker.js       serves the game and relays WebSocket messages between pilots in a room
wrangler.toml       Cloudflare config (static assets + one Durable Object class, SQLite-backed so it runs on the free plan)
```

## Deploy (free)

1. Make a free Cloudflare account: https://dash.cloudflare.com/sign-up
2. In this folder:
   ```
   npm install
   npx wrangler login
   npx wrangler deploy
   ```
3. Wrangler prints your address, something like `https://lowfly.<your-subdomain>.workers.dev`.
   Open it, pick an aircraft, type a room code, press **Join online**. Send the same address and code to friends.

## Try it locally first

```
npx wrangler dev
```
Open http://localhost:8787 in two browser windows, use the same room code in both.

## How it works

- Each pilot flies their own plane. The game sends position, orientation, velocity and control positions 15 times a second.
- The room (one Durable Object per room code, up to 8 pilots) forwards each message to the other pilots, stamped with the sender's id.
- Other pilots are drawn about 150 ms in the past and blended between snapshots, so network jitter doesn't show.
- Clocks are synced to the server, so the target drones orbit in the same place for everyone. A drone kill is broadcast so it disappears for everyone.
- Crashes are broadcast (others see the explosion and debris). Your accent colour is your slot colour; it is also your dot on the minimap.

## Free plan limits (check https://developers.cloudflare.com/durable-objects/platform/pricing)

- 100,000 Durable Object requests per day; incoming WebSocket messages count 20:1, so about 2 million messages a day.
  At 15 messages per second per pilot that is roughly 37 pilot-hours per day across all rooms.
- 13,000 GB-s of duration per day, about 28 hours of an active room per day (a room with nobody connected is free).
- Limits reset at 00:00 UTC. When one is exceeded, new connections fail until the reset.

Lower `SEND_HZ` in `public/index.html` (try 10) to stretch the allowance; raise `INTERP_MS` a little if remote planes look jerky.

## Not in this version

No pilot-vs-pilot weapons or damage yet (guns and missiles only affect the drones), and no collisions between planes.
Flights are client-authoritative: fine for friends, but a modified client could cheat.
