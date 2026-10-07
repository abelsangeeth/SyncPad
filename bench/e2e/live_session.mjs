import { RGA, decodeOps, encodeOps, decodeSnapshot } from '../../frontend/src/crdt.js';

const BASE = "http://127.0.0.1:8777";
const WS = "ws://127.0.0.1:8777/ws";

const j = async (method, path, body, token) => {
  const r = await fetch(BASE + path, {
    method,
    headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const t = await r.text();
  if (!r.ok) throw new Error(`${method} ${path} -> ${r.status} ${t}`);
  return t ? JSON.parse(t) : null;
};

class Client {
  constructor(name) { this.name = name; this.queue = []; this.waiters = []; }
  connect(doc, token) {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(WS);
      this.ws.onmessage = (e) => {
        const outer = JSON.parse(e.data);
        // The gateway coalesces frames under load; unwrap exactly as the
        // browser client does.
        for (const f of outer.t === 'batch' ? outer.frames : [outer]) this.handle(f, resolve);
      };
      this.handle = (f, resolve) => {
        if (f.t === 'welcome') {
          this.site = f.site; this.seq = f.seq; this.role = f.role;
          this.rga = f.snapshot ? RGA.load(f.site, decodeSnapshot(f.snapshot)) : new RGA(f.site);
          for (const b of f.ops || []) { this.rga.applyMany(decodeOps(b.ops)); this.seq = Math.max(this.seq, b.seq); }
          resolve(f); return;
        }
        if (f.t === 'ops') { this.rga.applyMany(decodeOps(f.ops)); this.seq = Math.max(this.seq, f.seq); }
        if (f.t === 'ack')  { this.seq = Math.max(this.seq, f.seq); }
        this.queue.push(f);
        const w = this.waiters.shift(); if (w) w();
      };
      this.ws.onerror = reject;
      this.ws.onopen = () => this.ws.send(JSON.stringify({ t: 'hello', doc, token, since: 0 }));
    });
  }
  type(i, text) { const ops = this.rga.localInsert(i, text); this.cseq = (this.cseq||0)+1;
    this.ws.send(JSON.stringify({ t: 'ops', ops: encodeOps(ops), cseq: this.cseq })); }
  del(i, n) { const ops = this.rga.localDelete(i, n); this.cseq = (this.cseq||0)+1;
    this.ws.send(JSON.stringify({ t: 'ops', ops: encodeOps(ops), cseq: this.cseq })); }
  mark(i, n, k, v) { const ops = this.rga.localMark(i, n, k, v); this.cseq = (this.cseq||0)+1;
    this.ws.send(JSON.stringify({ t: 'ops', ops: encodeOps(ops), cseq: this.cseq })); }
  async until(seq, ms = 4000) {
    const end = Date.now() + ms;
    while (this.seq < seq && Date.now() < end) {
      await new Promise((r) => { this.waiters.push(r); setTimeout(r, 60); });
    }
    if (this.seq < seq) throw new Error(`${this.name} stuck at seq ${this.seq}, wanted ${seq}`);
  }
  async ack(ms = 4000) {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      const i = this.queue.findIndex((f) => f.t === 'ack');
      if (i >= 0) return this.queue.splice(i, 1)[0].seq;
      await new Promise((r) => { this.waiters.push(r); setTimeout(r, 60); });
    }
    throw new Error(`${this.name} never got an ack`);
  }
  close() { this.ws.close(); }
}

const stamp = Date.now();
const acct = await j('POST', '/api/auth/register',
  { email: `e2e${stamp}@example.com`, name: 'E2E', password: 'hunter2-hunter2' });
const token = acct.access_token;
const doc = await j('POST', '/api/documents', { title: 'E2E Doc' }, token);
console.log('registered + created doc', doc.id);

const alice = new Client('alice'), bob = new Client('bob');
await alice.connect(doc.id, token);
await bob.connect(doc.id, token);
console.log('two clients connected; roles:', alice.role, bob.role);

alice.type(0, 'The quick brown fox ');
await bob.until(await alice.ack());

bob.type(bob.rga.length, 'jumps over the lazy dog.');
await alice.until(await bob.ack());

// concurrent edits at the same index, neither having seen the other
alice.type(4, '<A>');
bob.type(4, '<B>');
const top = Math.max(await alice.ack(), await bob.ack());
await alice.until(top); await bob.until(top);

alice.mark(0, 3, 'bold', true);
await bob.until(await alice.ack());

bob.del(0, 3);
await alice.until(await bob.ack());

console.log('\nalice:', JSON.stringify(alice.rga.text()));
console.log('bob  :', JSON.stringify(bob.rga.text()));
console.log('CLIENTS CONVERGED:', alice.rga.text() === bob.rga.text());
console.log('spans match       :', JSON.stringify(alice.rga.spans()) === JSON.stringify(bob.rga.spans()));

alice.close(); bob.close();
await new Promise((r) => setTimeout(r, 400));

const server = await j('GET', `/api/documents/${doc.id}`, undefined, token);
console.log('server:', JSON.stringify(server.text));
console.log('SERVER MATCHES CLIENTS:', server.text === alice.rga.text());

const md = await fetch(`${BASE}/api/documents/${doc.id}/export?format=md`, { headers: { Authorization: `Bearer ${token}` } });
console.log('markdown export:', JSON.stringify(await md.text()));
const hist = await j('GET', `/api/documents/${doc.id}/history`, undefined, token);
console.log('history: seq', hist.seq, '/', hist.revisions.length, 'revisions');
