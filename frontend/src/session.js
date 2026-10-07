/**
 * The connection: one document, one socket, and the buffer that makes it
 * survivable.
 *
 * The design assumption is that the socket is unreliable and the local replica
 * is the source of truth for what *this* user has typed. Edits apply to the
 * local RGA immediately and are queued for transmission; the queue drains on
 * acknowledgement. If the connection drops — a tunnel, a sleeping laptop, a
 * rolling deploy — typing continues against the local replica and the queue
 * grows. On reconnect the client sends `since`, receives only what it missed,
 * replays its unacknowledged queue, and converges.
 *
 * Replay is safe because operations are idempotent and commutative: the server
 * may well have already received and applied some of them before the socket
 * died. Nothing here needs to reason about that.
 */

import { RGA, decodeOps, decodeSnapshot, encodeOps } from './crdt.js';

const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 15000;
const HEARTBEAT_MS = 20000;
const CURSOR_THROTTLE_MS = 80;

export class Session extends EventTarget {
  constructor({ docId, token, share }) {
    super();
    this.docId = docId;
    this.token = token || null;
    this.share = share || null;

    this.socket = null;
    this.rga = null;
    this.site = null;
    this.role = 'viewer';
    this.user = null;
    this.title = '';
    this.peers = [];
    this.seq = 0;

    /** Operations applied locally but not yet acknowledged by the server. */
    this.outbox = [];
    this.clientSeq = 0;
    this.status = 'connecting';

    this._reconnectAttempts = 0;
    this._reconnectTimer = null;
    this._heartbeatTimer = null;
    this._cursorTimer = null;
    this._pendingCursor = null;
    this._closedByUs = false;
  }

  // --------------------------------------------------------------- lifecycle
  connect() {
    this._closedByUs = false;
    const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
    this.socket = new WebSocket(`${scheme}://${window.location.host}/ws`);

    this.socket.addEventListener('open', () => {
      this._setStatus('syncing');
      this._send({
        t: 'hello',
        doc: this.docId,
        token: this.token,
        share: this.share,
        // Zero means cold: send a snapshot. Anything else asks for the delta.
        since: this.seq,
      });
    });

    this.socket.addEventListener('message', (event) => this._onFrame(event.data));

    this.socket.addEventListener('close', () => {
      this._stopHeartbeat();
      if (this._closedByUs) return;
      this._setStatus('offline');
      this._scheduleReconnect();
    });

    this.socket.addEventListener('error', () => {
      // 'close' always follows, and that is where reconnection is handled.
      this._setStatus('offline');
    });
  }

  close() {
    this._closedByUs = true;
    this._stopHeartbeat();
    clearTimeout(this._reconnectTimer);
    clearTimeout(this._cursorTimer);
    if (this.socket) this.socket.close();
    this.socket = null;
  }

  _scheduleReconnect() {
    clearTimeout(this._reconnectTimer);
    // Exponential backoff with jitter: a server restart reconnects a hundred
    // editors at once, and without jitter they would retry in lockstep and
    // knock it over again.
    const backoff = Math.min(RECONNECT_BASE_MS * 2 ** this._reconnectAttempts, RECONNECT_MAX_MS);
    const delay = backoff * (0.5 + Math.random() * 0.5);
    this._reconnectAttempts += 1;
    this._reconnectTimer = setTimeout(() => this.connect(), delay);
  }

  _startHeartbeat() {
    this._stopHeartbeat();
    this._heartbeatTimer = setInterval(() => {
      if (this.socket && this.socket.readyState === WebSocket.OPEN) this._send({ t: 'ping' });
    }, HEARTBEAT_MS);
  }

  _stopHeartbeat() {
    if (this._heartbeatTimer) clearInterval(this._heartbeatTimer);
    this._heartbeatTimer = null;
  }

  // ----------------------------------------------------------------- frames
  _send(frame) {
    if (this.socket && this.socket.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify(frame));
      return true;
    }
    return false;
  }

  _onFrame(raw) {
    let frame;
    try {
      frame = JSON.parse(raw);
    } catch {
      return;
    }
    this._handle(frame);
  }

  _handle(frame) {
    switch (frame.t) {
      case 'batch':
        // The gateway coalesces frames that piled up in one event-loop tick.
        // Each inner frame is handled exactly as if it had arrived alone.
        for (const inner of frame.frames) this._handle(inner);
        return;

      case 'welcome':
        this._onWelcome(frame);
        break;

      case 'ops': {
        const ops = decodeOps(frame.ops);
        this.rga.applyMany(ops);
        this.seq = Math.max(this.seq, frame.seq);
        this._emit('remote', { ops, origin: frame.origin });
        break;
      }

      case 'ack':
        this.seq = Math.max(this.seq, frame.seq);
        // Everything up to this client sequence is durable; stop replaying it.
        this.outbox = this.outbox.filter((entry) => entry.cseq > frame.cseq);
        this._setStatus(this.outbox.length ? 'syncing' : 'synced');
        break;

      case 'presence':
        this.peers = frame.peers.filter((peer) => peer.site !== this.site);
        this._emit('presence', { peers: this.peers });
        break;

      case 'error':
        this._emit('error', { code: frame.code, message: frame.msg, fatal: frame.fatal });
        if (frame.fatal) this.close();
        break;

      case 'ping':
        this._send({ t: 'pong' });
        break;

      case 'pong':
        break;

      default:
        break;
    }
  }

  _onWelcome(frame) {
    this.site = frame.site;
    this.role = frame.role;
    this.user = frame.you;
    this.title = frame.title;
    this.peers = (frame.peers || []).filter((peer) => peer.site !== this.site);
    this._reconnectAttempts = 0;

    if (frame.snapshot) {
      // Cold start, or a reconnection too far behind to patch up.
      this.rga = RGA.load(this.site, decodeSnapshot(frame.snapshot));
    } else if (!this.rga) {
      this.rga = new RGA(this.site);
    } else {
      // Resumed: keep the replica we already had, but adopt the new site id so
      // operations minted after this point cannot collide with the old ones.
      this.rga.clock.site = this.site;
    }

    for (const batch of frame.ops || []) {
      this.rga.applyMany(decodeOps(batch.ops));
      this.seq = Math.max(this.seq, batch.seq);
    }
    this.seq = Math.max(this.seq, frame.seq);

    // Anything typed while disconnected goes back on the wire. Duplicates are
    // harmless: operations are idempotent.
    this._flushOutbox();
    this._startHeartbeat();
    this._setStatus(this.outbox.length ? 'syncing' : 'synced');
    this._emit('ready', { frame });
  }

  _flushOutbox() {
    for (const entry of this.outbox) {
      this._send({ t: 'ops', ops: encodeOps(entry.ops), cseq: entry.cseq });
    }
  }

  // ------------------------------------------------------------ local edits
  /** Queue locally generated operations for transmission. */
  submit(ops) {
    if (!ops || !ops.length) return;
    this.clientSeq += 1;
    const entry = { cseq: this.clientSeq, ops };
    this.outbox.push(entry);

    const sent = this._send({ t: 'ops', ops: encodeOps(ops), cseq: entry.cseq });
    this._setStatus(sent ? 'syncing' : 'offline');
    this._emit('local', { ops });
  }

  insert(index, text) {
    const ops = this.rga.localInsert(index, text);
    this.submit(ops);
    return ops;
  }

  remove(index, length) {
    const ops = this.rga.localDelete(index, length);
    this.submit(ops);
    return ops;
  }

  mark(index, length, key, value, expand = true) {
    const ops = this.rga.localMark(index, length, key, value, expand);
    this.submit(ops);
    return ops;
  }

  /**
   * Report the cursor, throttled.
   *
   * Cursor frames are the highest-frequency traffic in the system — every
   * arrow key produces one — and unlike operations they are disposable: only
   * the latest position matters, so coalescing is free.
   */
  sendCursor(anchorId, headId) {
    this._pendingCursor = {
      t: 'cursor',
      anchor: anchorId ? anchorId.encode() : null,
      head: headId ? headId.encode() : null,
    };
    if (this._cursorTimer) return;
    this._cursorTimer = setTimeout(() => {
      this._cursorTimer = null;
      if (this._pendingCursor) {
        this._send(this._pendingCursor);
        this._pendingCursor = null;
      }
    }, CURSOR_THROTTLE_MS);
  }

  get canEdit() {
    return this.role === 'owner' || this.role === 'editor';
  }

  get canComment() {
    return this.canEdit || this.role === 'commenter';
  }

  get pendingCount() {
    return this.outbox.reduce((total, entry) => total + entry.ops.length, 0);
  }

  // ------------------------------------------------------------------ events
  _setStatus(status) {
    if (this.status === status) return;
    this.status = status;
    this._emit('status', { status, pending: this.pendingCount });
  }

  _emit(name, detail) {
    this.dispatchEvent(new CustomEvent(name, { detail }));
  }
}
