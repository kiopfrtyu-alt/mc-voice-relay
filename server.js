const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
// ตั้ง RELAY_SECRET ในตัวแปรสภาพแวดล้อมของโฮสต์ (แนะนำ) ถ้าไม่ตั้ง รีเลย์จะใช้รหัสของคนที่ส่งมาก่อนในแต่ละห้อง
const ENV_SECRET = process.env.RELAY_SECRET || "";
if (!ENV_SECRET) console.warn("คำเตือน: ยังไม่ได้ตั้ง RELAY_SECRET ใครรู้รหัสห้องก็ยึดห้องได้ถ้าส่งก่อนเซิร์ฟ");

const rooms = new Map();
const mkRoom = (id) => {
  if (!rooms.has(id)) rooms.set(id, { secret: null, players: Object.create(null), codes: new Map(), clients: new Map(), fails: new Map(), updated: 0 });
  return rooms.get(id);
};
const sendRaw = (ws, s) => ws.readyState === 1 && ws.send(s);
const send = (ws, m) => sendRaw(ws, JSON.stringify(m));
const same = (a, b) => {
  const x = Buffer.from(String(a)), y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
};
const num = (v) => (typeof v === "number" && isFinite(v) ? v : 0);

const server = http.createServer((req, res) => {
  if (req.method === "POST" && req.url === "/ingest") {
    let b = "", dead = false;
    req.on("data", (c) => { b += c; if (b.length > 2e5 && !dead) { dead = true; res.writeHead(413); res.end(); req.destroy(); } });
    req.on("end", () => {
      if (dead) return;
      try {
        const d = JSON.parse(b);
        if (!d.room || !d.secret || !Array.isArray(d.players)) throw new Error("bad");
        if (ENV_SECRET && !same(d.secret, ENV_SECRET)) { res.writeHead(403); return res.end(); }
        const r = mkRoom(String(d.room).slice(0, 64));
        if (!ENV_SECRET) {
          if (r.secret === null) r.secret = String(d.secret);
          else if (!same(r.secret, d.secret)) { res.writeHead(403); return res.end(); }
        }
        const players = Object.create(null), codes = new Map();
        for (const p of d.players.slice(0, 200)) {
          if (!p || typeof p.name !== "string" || !p.name) continue;
          const name = p.name.slice(0, 32);
          players[name] = {
            name, x: num(p.x), y: num(p.y), z: num(p.z), dx: num(p.dx), dy: num(p.dy), dz: num(p.dz),
            dim: String(p.dim || "").slice(0, 40), water: p.water === true, covered: p.covered === true, sneak: p.sneak === true,
          };
          if (p.code) codes.set(name, String(p.code).slice(0, 12)); // รหัสจับคู่ ห้ามส่งต่อให้ผู้เล่นคนอื่น
        }
        r.players = players; r.codes = codes; r.updated = Date.now();
        res.writeHead(204); res.end();
      } catch (e) { res.writeHead(400); res.end(); }
    });
    return;
  }
  if (req.url.split("?")[0] === "/") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    // รองรับทั้งไฟล์ index.html วางข้าง server.js และในโฟลเดอร์ public
    const flat = path.join(__dirname, "index.html");
    return fs.createReadStream(fs.existsSync(flat) ? flat : path.join(__dirname, "public", "index.html")).pipe(res);
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
wss.on("connection", (ws, req) => {
  const u = new URL(req.url, "http://x");
  const id = u.searchParams.get("room"), name = u.searchParams.get("name"), code = u.searchParams.get("code");
  // เหตุผลตอนปิดต้องไม่เกิน 123 ไบต์ (ภาษาไทยตัวละ 3 ไบต์) จึงส่งข้อความไทยทาง message แล้วปิดด้วยเหตุผลสั้น ๆ
  const reject = (c, why) => { send(ws, { t: "error", code: c, why }); ws.close(c, "rejected"); };
  if (!id || !name || !code) return reject(4000, "ข้อมูลไม่ครบ");
  const r = rooms.get(id);
  if (!r || Date.now() - r.updated > 15000) return reject(4004, "ห้องนี้ยังไม่มีข้อมูลจากเกม (เซิร์ฟเวอร์ยังไม่ส่งมา)");

  // จำกัดการเดารหัส: ผิดเกิน 5 ครั้งต่อนาทีต่อชื่อ
  const f = r.fails.get(name) || { n: 0, t: Date.now() };
  if (Date.now() - f.t > 60000) { f.n = 0; f.t = Date.now(); }
  if (f.n >= 5) return reject(4029, "ลองรหัสผิดหลายครั้ง รอ 1 นาทีแล้วลองใหม่");
  if (!r.players[name]) return reject(4001, "ไม่พบชื่อนี้ในเกม (ต้องเข้าเกมก่อน และพิมพ์ชื่อให้ตรงตัวพิมพ์)");
  const want = r.codes.get(name);
  if (!want || !same(want, code)) { f.n++; r.fails.set(name, f); return reject(4003, "รหัสไม่ถูกต้อง (ดูรหัสในแชตเกม หรือพิมพ์ /scriptevent voice:code)"); }
  r.fails.delete(name);

  if (r.clients.has(name)) { const old = r.clients.get(name); send(old, { t: "error", code: 4005, why: "มีการเข้าห้องด้วยชื่อนี้จากอุปกรณ์อื่น" }); old.close(4005, "replaced"); }
  r.clients.set(name, ws);
  send(ws, { t: "peers", peers: [...r.clients.keys()].filter((n) => n !== name) });
  ws.on("message", (m) => {
    try {
      const d = JSON.parse(m);
      if (d.t === "signal" && typeof d.to === "string") { const to = r.clients.get(d.to); if (to) send(to, { t: "signal", from: name, data: d.data }); }
    } catch (e) {}
  });
  ws.on("close", () => {
    if (r.clients.get(name) === ws) {
      r.clients.delete(name);
      for (const c of r.clients.values()) send(c, { t: "leave", name });
    }
  });
});

setInterval(() => {
  for (const r of rooms.values()) {
    if (!r.clients.size) continue;
    const s = JSON.stringify({ t: "state", players: r.players, fresh: Date.now() - r.updated < 5000 });
    for (const c of r.clients.values()) sendRaw(c, s);
  }
}, 250);

server.listen(PORT, () => console.log("relay on :" + PORT));
