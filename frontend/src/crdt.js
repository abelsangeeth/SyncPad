/**
 * The RGA, in the browser.
 *
 * This is a deliberate reimplementation of `syncpad/crdt/rga.py`, not a port of
 * convenience. The client holds a full replica so that typing applies locally
 * and instantly — a keystroke never waits for a server round trip — and so that
 * editing continues while offline, reconciling on reconnect.
 *
 * Because both replicas must converge, the two implementations have to agree on
 * three things exactly:
 *
 *   1. the total order on operation ids — `(counter, site)`, ascending;
 *   2. the integration rule — scan right from the left origin, skipping every
 *      element whose id sorts above the new one;
 *   3. the wire format — the short keys in `encodeOp` / `decodeOp`.
 *
 * Any divergence in those three is a split-brain document. The property tests
 * in `backend/tests/test_rga_convergence.py` pin the Python side; the
 * conformance check in `bench/conformance.py` feeds identical operation logs
 * through both and asserts identical text.
 *
 * The index structure differs on purpose. The server uses an order-statistic
 * treap because it holds many documents and resolves anchors constantly; a tab
 * holds one document, so this uses a plain array with a lazily rebuilt id→index
 * map. That is O(n) per rebuild rather than O(log n) per lookup, which for a
 * single document of realistic size is faster in practice and far less code.
 */

const MAX_INSERT_RUN = 512;

/**
 * Split a string into code points, never UTF-16 code units.
 *
 * This is load-bearing, not a nicety. JavaScript indexes strings by UTF-16 unit,
 * so `"🌍".length` is 2 and `"🌍"[0]` is half a surrogate pair; Python indexes by
 * code point, so the same emoji is one character. Using the native indexing here
 * would give the two replicas *different numbers of elements for the same text* —
 * each half of the pair would get its own id and could be reordered or deleted
 * independently, producing mojibake that no amount of CRDT correctness can undo.
 *
 * The cross-language check in `bench/conformance.py` catches exactly this.
 */
const codePoints = (text) => Array.from(text);

/**
 * An order-independent signature for a mark set.
 *
 * Two adjacent characters carrying the same formatting must join one span. The
 * obvious `JSON.stringify` comparison is key-order sensitive, so `{bold,italic}`
 * and `{italic,bold}` — which arise naturally depending on the order the marks
 * became active during the sweep — would read as different and split the run.
 * Python compares dicts, where order never mattered, so the two implementations
 * would disagree about span boundaries while agreeing about the text.
 */
const markSignature = (marks) =>
  Object.keys(marks)
    .sort()
    .map((key) => `${key}=${JSON.stringify(marks[key])}`)
    .join('|');

export const MARK_KEYS = ['bold', 'italic', 'code', 'strike', 'link', 'heading'];

/** A globally unique, totally ordered identifier for one character. */
export class OpId {
  constructor(site, counter) {
    this.site = site;
    this.counter = counter;
  }

  static decode(text) {
    const cut = text.lastIndexOf(':');
    if (cut <= 0) throw new Error(`malformed OpId ${text}`);
    return new OpId(text.slice(0, cut), Number(text.slice(cut + 1)));
  }

  encode() {
    return `${this.site}:${this.counter}`;
  }

  offset(n) {
    return new OpId(this.site, this.counter + n);
  }

  /** Ascending by (counter, site) — identical to the Python ordering. */
  gt(other) {
    if (this.counter !== other.counter) return this.counter > other.counter;
    return this.site > other.site;
  }

  lt(other) {
    if (this.counter !== other.counter) return this.counter < other.counter;
    return this.site < other.site;
  }

  eq(other) {
    return this.counter === other.counter && this.site === other.site;
  }
}

/** Per-site Lamport clock. */
class LamportClock {
  constructor(site, counter = 0) {
    this.site = site;
    this.counter = counter;
  }

  tick() {
    this.counter += 1;
    return new OpId(this.site, this.counter);
  }

  tickRun(n) {
    const start = this.counter + 1;
    this.counter += n;
    return new OpId(this.site, start);
  }

  observe(id) {
    if (id.counter > this.counter) this.counter = id.counter;
  }
}

export function encodeOp(op) {
  if (op.kind === 'ins') {
    return { k: 'ins', id: op.id.encode(), after: op.after ? op.after.encode() : null, tx: op.text };
  }
  if (op.kind === 'del') {
    return { k: 'del', id: op.id.encode(), tg: op.targets.map((t) => t.encode()) };
  }
  return {
    k: 'mark',
    id: op.id.encode(),
    s: op.start.encode(),
    e: op.end.encode(),
    key: op.key,
    v: op.value,
    x: op.expand,
  };
}

export function decodeOp(raw) {
  if (raw.k === 'ins') {
    return {
      kind: 'ins',
      id: OpId.decode(raw.id),
      after: raw.after ? OpId.decode(raw.after) : null,
      text: raw.tx,
    };
  }
  if (raw.k === 'del') {
    return { kind: 'del', id: OpId.decode(raw.id), targets: raw.tg.map(OpId.decode) };
  }
  if (raw.k === 'mark') {
    return {
      kind: 'mark',
      id: OpId.decode(raw.id),
      start: OpId.decode(raw.s),
      end: OpId.decode(raw.e),
      key: raw.key,
      value: raw.v === undefined ? null : raw.v,
      expand: raw.x === undefined ? true : raw.x,
    };
  }
  throw new Error(`unknown operation kind ${raw.k}`);
}

export const encodeOps = (ops) => ops.map(encodeOp);
export const decodeOps = (raw) => raw.map(decodeOp);

export class RGA {
  constructor(site) {
    this.clock = new LamportClock(site);
    /** Elements in document order, tombstones included. */
    this.elements = [];
    /** id string -> element. */
    this.byId = new Map();
    /** id string -> array index; rebuilt lazily after structural changes. */
    this.index = new Map();
    this.indexDirty = true;
    /** Deletes for characters not yet received. */
    this.pendingDeletes = new Set();
    /** Inserts awaiting their left origin, keyed by the origin's id string. */
    this.pendingInserts = new Map();
    this.marks = new Map();
    this.versionVector = new Map();
  }

  get site() {
    return this.clock.site;
  }

  _reindex() {
    if (!this.indexDirty) return;
    this.index.clear();
    for (let i = 0; i < this.elements.length; i += 1) {
      this.index.set(this.elements[i].id.encode(), i);
    }
    this.indexDirty = false;
  }

  _arrayIndexOf(id) {
    this._reindex();
    const found = this.index.get(id.encode());
    return found === undefined ? -1 : found;
  }

  // ------------------------------------------------------------- reading
  text() {
    let out = '';
    for (const element of this.elements) if (!element.deleted) out += element.ch;
    return out;
  }

  get length() {
    let n = 0;
    for (const element of this.elements) if (!element.deleted) n += 1;
    return n;
  }

  /** Visible index of a character, or null if unknown or deleted. */
  indexOf(id) {
    const at = this._arrayIndexOf(id);
    if (at < 0 || this.elements[at].deleted) return null;
    let visible = 0;
    for (let i = 0; i < at; i += 1) if (!this.elements[i].deleted) visible += 1;
    return visible;
  }

  /**
   * Where a character is, or would be had it survived.
   *
   * A cursor may be anchored to text someone else has since deleted. Returning
   * null there would throw the caret to the top of the document mid-keystroke;
   * reporting the tombstone's position keeps it where the user left it.
   */
  anchorIndex(id) {
    const at = this._arrayIndexOf(id);
    if (at < 0) return null;
    let visible = 0;
    for (let i = 0; i < at; i += 1) if (!this.elements[i].deleted) visible += 1;
    return visible;
  }

  idAt(visibleIndex) {
    let seen = 0;
    for (const element of this.elements) {
      if (element.deleted) continue;
      if (seen === visibleIndex) return element.id;
      seen += 1;
    }
    return null;
  }

  /** Visible text split into maximal runs of identical formatting. */
  spans() {
    const resolved = this._resolveMarks();
    const out = [];
    let buffer = '';
    let currentMarks = null;
    let currentKey = null;

    for (let i = 0; i < this.elements.length; i += 1) {
      const element = this.elements[i];
      if (element.deleted) continue;
      const marks = resolved[i] || {};
      const key = markSignature(marks);
      if (currentKey === null) {
        currentKey = key;
        currentMarks = marks;
      } else if (key !== currentKey) {
        out.push({ text: buffer, marks: currentMarks });
        buffer = '';
        currentKey = key;
        currentMarks = marks;
      }
      buffer += element.ch;
    }
    if (buffer) out.push({ text: buffer, marks: currentMarks || {} });
    return out;
  }

  marksAt(visibleIndex) {
    const id = this.idAt(visibleIndex);
    if (!id) return {};
    const at = this._arrayIndexOf(id);
    const resolved = this._resolveMarks();
    return resolved[at] || {};
  }

  /**
   * Per-array-position mark sets, by sweeping interval events.
   *
   * Marks are anchored to character ids, so coverage is recomputed against
   * current document order every time. That is what lets a span keep its
   * meaning while text is inserted and deleted inside and around it.
   */
  _resolveMarks() {
    const resolved = new Array(this.elements.length);
    if (this.marks.size === 0) return resolved;
    this._reindex();

    const events = [];
    for (const mark of this.marks.values()) {
      const lo = this._arrayIndexOf(mark.start);
      const hi = this._arrayIndexOf(mark.end);
      if (lo < 0 || hi < 0) continue; // anchors not delivered yet
      events.push({ at: Math.min(lo, hi), delta: 1, mark });
      events.push({ at: Math.max(lo, hi) + 1, delta: -1, mark });
    }
    if (!events.length) return resolved;
    events.sort((a, b) => a.at - b.at || a.delta - b.delta);

    const active = new Map();
    let cursor = 0;
    for (let i = 0; i < this.elements.length; i += 1) {
      while (cursor < events.length && events[cursor].at <= i) {
        const { delta, mark } = events[cursor];
        cursor += 1;
        const key = mark.key;
        if (!active.has(key)) active.set(key, new Map());
        const bucket = active.get(key);
        if (delta > 0) bucket.set(mark.id.encode(), mark);
        else {
          bucket.delete(mark.id.encode());
          if (!bucket.size) active.delete(key);
        }
      }
      if (this.elements[i].deleted || !active.size) continue;

      const element = this.elements[i];
      const marks = {};
      for (const [key, bucket] of active) {
        let best = null;
        for (const mark of bucket.values()) {
          // expand=false covers only text that already existed, in id order,
          // when the mark was made — matching the Python resolver exactly.
          if (!mark.expand && !element.id.lt(mark.id)) continue;
          if (best === null || mark.id.gt(best.id)) best = mark;
        }
        if (best && best.value !== null && best.value !== false) marks[key] = best.value;
      }
      if (Object.keys(marks).length) resolved[i] = marks;
    }
    return resolved;
  }

  /** Visible text between two anchors, inclusive. */
  textBetween(startId, endId) {
    const a = this._arrayIndexOf(startId);
    const b = this._arrayIndexOf(endId);
    if (a < 0 || b < 0) return '';
    const lo = Math.min(a, b);
    const hi = Math.max(a, b);
    let out = '';
    for (let i = lo; i <= hi; i += 1) if (!this.elements[i].deleted) out += this.elements[i].ch;
    return out;
  }

  // ------------------------------------------------------- local editing
  localInsert(visibleIndex, text) {
    if (!text) return [];
    const chars = codePoints(text);
    let after = visibleIndex > 0 ? this.idAt(visibleIndex - 1) : null;
    const ops = [];
    for (let offset = 0; offset < chars.length; offset += MAX_INSERT_RUN) {
      const slice = chars.slice(offset, offset + MAX_INSERT_RUN);
      const op = {
        kind: 'ins',
        id: this.clock.tickRun(slice.length),
        after,
        text: slice.join(''),
      };
      this.apply(op);
      ops.push(op);
      after = op.id.offset(slice.length - 1);
    }
    return ops;
  }

  localDelete(visibleIndex, length) {
    if (length <= 0) return [];
    const targets = [];
    let seen = 0;
    for (const element of this.elements) {
      if (element.deleted) continue;
      if (seen >= visibleIndex && targets.length < length) targets.push(element.id);
      seen += 1;
      if (targets.length >= length) break;
    }
    if (!targets.length) return [];

    const ops = [];
    for (let i = 0; i < targets.length; i += MAX_INSERT_RUN) {
      const op = {
        kind: 'del',
        id: this.clock.tick(),
        targets: targets.slice(i, i + MAX_INSERT_RUN),
      };
      this.apply(op);
      ops.push(op);
    }
    return ops;
  }

  localMark(visibleIndex, length, key, value, expand = true) {
    if (length <= 0) return [];
    const start = this.idAt(visibleIndex);
    const end = this.idAt(visibleIndex + length - 1);
    if (!start || !end) return [];
    const op = { kind: 'mark', id: this.clock.tick(), start, end, key, value, expand };
    this.apply(op);
    return [op];
  }

  // ------------------------------------------------------ remote editing
  applyMany(ops) {
    let changed = 0;
    for (const op of ops) if (this.apply(op)) changed += 1;
    return changed;
  }

  apply(op) {
    if (op.kind === 'ins') return this._applyInsert(op);
    if (op.kind === 'del') return this._applyDelete(op);
    return this._applyMark(op);
  }

  _observe(id, span = 1) {
    const last = id.counter + span - 1;
    this.clock.observe(new OpId(id.site, last));
    if (last > (this.versionVector.get(id.site) || 0)) this.versionVector.set(id.site, last);
  }

  _applyInsert(op) {
    this._observe(op.id, codePoints(op.text).length);
    if (this.byId.has(op.id.encode())) return false;

    if (op.after && !this.byId.has(op.after.encode())) {
      // Causally early: hold it until its left origin arrives.
      const key = op.after.encode();
      const waiting = this.pendingInserts.get(key) || [];
      if (waiting.some((w) => w.id.eq(op.id))) return false;
      waiting.push(op);
      this.pendingInserts.set(key, waiting);
      return false;
    }

    this._integrateRun(op);
    this._drain(op);
    return true;
  }

  _integrateRun(op) {
    const chars = codePoints(op.text);
    let parentId = op.after;
    for (let i = 0; i < chars.length; i += 1) {
      const id = op.id.offset(i);
      if (this.byId.has(id.encode())) {
        parentId = id;
        continue;
      }
      this._integrateOne(id, chars[i], parentId);
      parentId = id;
    }
  }

  /** The RGA integration rule — must match the Python implementation exactly. */
  _integrateOne(id, ch, parentId) {
    this._reindex();
    let at = parentId === null ? 0 : this._arrayIndexOf(parentId) + 1;
    while (at < this.elements.length && this.elements[at].id.gt(id)) at += 1;

    const deleted = this.pendingDeletes.has(id.encode());
    const element = { id, ch, parent: parentId, deleted };
    this.elements.splice(at, 0, element);
    this.byId.set(id.encode(), element);
    this.indexDirty = true;
    if (deleted) this.pendingDeletes.delete(id.encode());
  }

  /** Integrate inserts that were waiting on characters this run supplied. */
  _drain(op) {
    const frontier = [];
    const runLength = codePoints(op.text).length;
    for (let i = 0; i < runLength; i += 1) frontier.push(op.id.offset(i).encode());

    while (frontier.length) {
      const anchor = frontier.pop();
      const waiting = this.pendingInserts.get(anchor);
      if (!waiting) continue;
      this.pendingInserts.delete(anchor);
      for (const pending of waiting) {
        if (this.byId.has(pending.id.encode())) continue;
        this._integrateRun(pending);
        const pendingLength = codePoints(pending.text).length;
        for (let i = 0; i < pendingLength; i += 1) {
          frontier.push(pending.id.offset(i).encode());
        }
      }
    }
  }

  _applyDelete(op) {
    this._observe(op.id);
    let changed = false;
    for (const target of op.targets) {
      const key = target.encode();
      const element = this.byId.get(key);
      if (!element) {
        if (!this.pendingDeletes.has(key)) {
          this.pendingDeletes.add(key);
          changed = true;
        }
        continue;
      }
      if (!element.deleted) {
        element.deleted = true;
        changed = true;
      }
    }
    return changed;
  }

  _applyMark(op) {
    this._observe(op.id);
    const key = op.id.encode();
    if (this.marks.has(key)) return false;
    this.marks.set(key, op);
    return true;
  }

  // -------------------------------------------------------- persistence
  /** Load from a server snapshot. Format must match `RGA.snapshot` in Python. */
  static load(site, payload) {
    if (payload.v !== 1) throw new Error(`unsupported snapshot format ${payload.v}`);
    const rga = new RGA(site);

    for (const [elemSite, counter, ch, parent, deleted] of payload.elements) {
      const id = new OpId(elemSite, counter);
      const element = {
        id,
        ch,
        parent: parent ? OpId.decode(parent) : null,
        deleted: Boolean(deleted),
      };
      rga.elements.push(element);
      rga.byId.set(id.encode(), element);
    }
    rga.indexDirty = true;

    for (const raw of payload.marks || []) {
      const mark = decodeOp(raw);
      rga.marks.set(mark.id.encode(), mark);
    }
    for (const encoded of payload.pending_deletes || []) rga.pendingDeletes.add(encoded);
    for (const raw of payload.pending_inserts || []) {
      const op = decodeOp(raw);
      if (!op.after) continue;
      const key = op.after.encode();
      const waiting = rga.pendingInserts.get(key) || [];
      waiting.push(op);
      rga.pendingInserts.set(key, waiting);
    }

    let highest = 0;
    for (const [siteId, counter] of Object.entries(payload.vv || {})) {
      rga.versionVector.set(siteId, counter);
      if (counter > highest) highest = counter;
    }
    for (const element of rga.elements) {
      if (element.id.counter > highest) highest = element.id.counter;
    }
    // The clock must outrank everything already present, or locally typed text
    // sorts below existing characters and lands in the wrong place.
    rga.clock = new LamportClock(site, highest);
    return rga;
  }
}

/** Decode a base64 snapshot blob into the object `RGA.load` expects. */
export function decodeSnapshot(base64) {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return JSON.parse(new TextDecoder().decode(bytes));
}
