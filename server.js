const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
// ตั้ง RELAY_SECRET ในตัวแปรสภาพแวดล้อมของโฮสต์ (แนะนำ) ถ้าไม่ตั้ง รีเลย์จะใช้รหัสของคนที่ส่งมาก่อนในแต่ละห้อง
const ENV_SECRET = process.env.RELAY_SECRET || "";
if (!ENV_SECRET) console.warn("คำเตือน: ยังไม่ได้ตั้ง RELAY_SECRET ใครรู้รหัสห้องก็ยึดห้องได้ถ้าส่งก่อนเซิร์ฟ");

const rooms = new Map();
const ipFails = new Map();
setInterval(() => { const n = Date.now(); for (const [k, v] of ipFails) if (n - v.t > 60000) ipFails.delete(k); }, 60000);
const mkRoom = (id) => {
  if (!rooms.has(id)) rooms.set(id, { secret: null, players: Object.create(null), codes: new Map(), codesReady: false, settings: new Map(), admins: new Set(), pub: new Set(), clients: new Map(), fails: new Map(), updated: 0 });
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
        const players = Object.create(null), legacy = new Map();
        for (const p of d.players.slice(0, 200)) {
          if (!p || typeof p.name !== "string" || !p.name) continue;
          const name = p.name.slice(0, 32);
          players[name] = {
            name, x: num(p.x), y: num(p.y), z: num(p.z), dx: num(p.dx), dy: num(p.dy), dz: num(p.dz),
            dim: String(p.dim || "").slice(0, 40), water: p.water === true, covered: p.covered === true, sneak: p.sneak === true,
          };
          if (p.code) legacy.set(name, String(p.code).slice(0, 12)); // แอดออนรุ่นเก่า: ส่งรหัสมากับผู้เล่นออนไลน์
        }
        // แอดออนรุ่นใหม่ส่ง codes = { ชื่อ: รหัส } ของทุกคนที่เคยเข้าเกม (รวมคนที่ออฟไลน์) รหัสห้ามส่งต่อให้ผู้เล่นคนอื่น
        if (d.codes && typeof d.codes === "object" && !Array.isArray(d.codes)) {
          const m = new Map();
          for (const [n, c] of Object.entries(d.codes).slice(0, 5000)) {
            if (typeof n === "string" && n && n.length <= 32 && /^\d{4,12}$/.test(String(c))) m.set(n.toLowerCase(), { name: n, code: String(c) });
          }
          r.codes = m; r.codesReady = true;
        } else if (legacy.size) {
          const m = new Map();
          for (const [n, c] of legacy) m.set(n.toLowerCase(), { name: n, code: c });
          r.codes = m; r.codesReady = true;
        }
        r.players = players; r.updated = Date.now();
        // รายชื่อผู้มีสิทธิ์ประกาศทั่วโลก (OP/แท็ก) ที่แอดออนตัดสินให้ ไม่เชื่อค่าจากฝั่งแอป
        if (Array.isArray(d.admins)) r.admins = new Set(d.admins.slice(0, 200).filter((n) => typeof n === "string" && n.length <= 32));
        // คำสั่งจากในเกม (ไอเทม/เมนู) ส่งต่อให้แอปของผู้เล่นคนนั้น: mute | range | speak | vol | pub
        if (Array.isArray(d.cmds)) {
          for (const c of d.cmds.slice(0, 50)) {
            if (!c || typeof c.name !== "string") continue;
            const ws = r.clients.get(c.name);
            if (!ws) continue;
            if (c.op === "mute" && typeof c.value === "boolean") send(ws, { t: "cmd", op: "mute", value: c.value });
            else if (c.op === "range" && isFinite(c.value)) send(ws, { t: "cmd", op: "range", value: Math.max(8, Math.min(64, +c.value)) });
            else if (c.op === "speak" && isFinite(c.value)) send(ws, { t: "cmd", op: "speak", value: Math.max(2, Math.min(64, +c.value)) });
            else if (c.op === "pub" && typeof c.value === "boolean") { if (c.value && r.admins.has(c.name)) r.pub.add(c.name); else r.pub.delete(c.name); }
            else if (c.op === "vol" && typeof c.target === "string" && isFinite(c.value)) send(ws, { t: "cmd", op: "vol", target: c.target.slice(0, 32), value: Math.max(0, Math.min(200, +c.value)) });
          }
        }
        // ตอบกลับสถานะการตั้งค่าของผู้เล่นที่เชื่อมแอปอยู่ (แอดออนใช้โชว์ในเมนู) ไม่มีรหัสในนี้
        const settings = {};
        for (const n of r.clients.keys()) settings[n] = Object.assign({ muted: false, range: 32, speak: 32, vols: {} }, r.settings.get(n), { pub: r.pub.has(n), admin: r.admins.has(n) });
        res.writeHead(200, { "Content-Type": "application/json" }); res.end(JSON.stringify({ settings }));
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
  let name = u.searchParams.get("name"); const id = u.searchParams.get("room"), code = u.searchParams.get("code");
  // เหตุผลตอนปิดต้องไม่เกิน 123 ไบต์ (ภาษาไทยตัวละ 3 ไบต์) จึงส่งข้อความไทยทาง message แล้วปิดด้วยเหตุผลสั้น ๆ
  const reject = (c, why) => { send(ws, { t: "error", code: c, why }); ws.close(c, "rejected"); };
  if (!id || !name || !code) return reject(4000, "ข้อมูลไม่ครบ");
  const r = rooms.get(id);
  // เซิร์ฟเวอร์ต้องออนไลน์ (ส่งข้อมูลมาในช่วง 15 วินาทีล่าสุด) แต่ผู้เล่นไม่ต้องอยู่ในเกม: เชื่อมรอไว้ได้
  if (!r || Date.now() - r.updated > 15000 || !r.codesReady) return reject(4004, "เซิร์ฟเวอร์เกมยังไม่ออนไลน์ หรือรีเลย์เพิ่งตื่น กำลังรอข้อมูลจากเกม");

  // จำกัดการเดารหัส: ผิดเกิน 5 ครั้งต่อนาทีต่อชื่อ และเกิน 30 ครั้งต่อนาทีต่อ IP
  const ip = String(req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").split(",")[0].trim();
  const key = name.toLowerCase();
  const lim = (m, k) => { let f = m.get(k); if (!f || Date.now() - f.t > 60000) { f = { n: 0, t: Date.now() }; m.set(k, f); } return f; };
  const f = lim(r.fails, key), fi = lim(ipFails, id + "|" + ip);
  if (f.n >= 5 || fi.n >= 30) return reject(4029, "ลองรหัสผิดหลายครั้ง รอ 1 นาทีแล้วลองใหม่");
  const ent = r.codes.get(key);
  // ชื่อไม่มีในระบบ กับรหัสผิด ตอบเหมือนกัน เพื่อไม่ให้เดาว่ามีชื่อใครบ้าง
  if (!ent || !same(ent.code, code)) { f.n++; fi.n++; return reject(4003, "ชื่อหรือรหัสไม่ถูกต้อง (ต้องเข้าเกมเซิร์ฟเวอร์นี้อย่างน้อยหนึ่งครั้งเพื่อรับรหัส ดูรหัสด้วย /scriptevent voice:code)"); }
  r.fails.delete(key);
  const canon = ent.name; // ใช้ชื่อตามที่เกมเก็บไว้ (ตัวพิมพ์ใหญ่เล็กตรงเกม)
  name = canon;

  if (r.clients.has(name)) { const old = r.clients.get(name); send(old, { t: "error", code: 4005, why: "มีการเข้าห้องด้วยชื่อนี้จากอุปกรณ์อื่น" }); old.close(4005, "replaced"); }
  r.clients.set(name, ws);
  send(ws, { t: "peers", name, peers: [...r.clients.keys()].filter((n) => n !== name) });
  ws.on("message", (m) => {
    try {
      const d = JSON.parse(m);
      if (d.t === "signal" && typeof d.to === "string") { const to = r.clients.get(d.to); if (to) send(to, { t: "signal", from: name, data: d.data }); }
      else if (d.t === "set") {
        const vols = {};
        if (d.vols && typeof d.vols === "object") for (const [k, v] of Object.entries(d.vols).slice(0, 100)) if (isFinite(v)) vols[String(k).slice(0, 32)] = Math.max(0, Math.min(200, +v));
        r.settings.set(name, { muted: d.muted === true, range: Math.max(8, Math.min(64, +d.range || 32)), speak: Math.max(2, Math.min(64, +d.speak || 32)), vols });
      } else if (d.t === "pub" && typeof d.value === "boolean") {
        // เปิด/ปิดไมค์ประกาศจากแอป: เฉพาะผู้มีสิทธิ์ (รายชื่อมาจากแอดออน)
        if (d.value && r.admins.has(name)) r.pub.add(name); else r.pub.delete(name);
      }
    } catch (e) {}
  });
  ws.on("close", () => {
    if (r.clients.get(name) === ws) {
      r.clients.delete(name); r.settings.delete(name); r.pub.delete(name);
      for (const c of r.clients.values()) send(c, { t: "leave", name });
    }
  });
});

setInterval(() => {
  for (const r of rooms.values()) {
    if (!r.clients.size) continue;
    for (const n of [...r.pub]) if (!r.admins.has(n) || !r.clients.has(n)) r.pub.delete(n); // หมดสิทธิ์/หลุด = เลิกประกาศ
    const speak = {}, pub = {};
    for (const n of r.clients.keys()) { const st = r.settings.get(n); if (st) speak[n] = st.speak; if (r.pub.has(n)) pub[n] = true; }
    const s = JSON.stringify({ t: "state", players: r.players, fresh: Date.now() - r.updated < 5000, speak, pub });
    for (const [n, c] of r.clients) {
      sendRaw(c, s);
      const me = JSON.stringify({ t: "me", admin: r.admins.has(n), pub: r.pub.has(n) });
      if (c._me !== me) { c._me = me; sendRaw(c, me); }
    }
  }
}, 250);

server.listen(PORT, () => console.log("relay on :" + PORT));
