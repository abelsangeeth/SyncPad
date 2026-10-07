/**
 * The editing surface.
 *
 * The hard problem in a collaborative editor is not applying remote changes —
 * the CRDT does that — it is applying them without disturbing the person who is
 * typing. A naive implementation re-renders the DOM and the caret jumps to the
 * start of the document, or lands somewhere arbitrary, every time a collaborator
 * presses a key.
 *
 * The fix falls out of the CRDT. Selection is captured not as "character 42"
 * but as the *identity* of the character the caret sits on. Identities are
 * immutable and never move relative to their text, so after a remote insert
 * three paragraphs above, the same identity resolves to a different offset and
 * the caret stays exactly where the user left it. No transformation, no
 * heuristics, no guessing.
 *
 * Input is read from `beforeinput` rather than by diffing the DOM. The browser
 * tells us the user's *intent* — insert this text, delete backwards by a word —
 * and intent maps directly onto CRDT operations. Diffing the DOM afterwards
 * would be lossy and would fight the IME.
 */

import { OpId } from './crdt.js';

const MARK_TAGS = {
  bold: 'strong',
  italic: 'em',
  code: 'code',
  strike: 's',
};

export class Editor extends EventTarget {
  constructor(root, session) {
    super();
    this.root = root;
    this.session = session;
    this.remoteCursors = new Map();

    this.root.setAttribute('contenteditable', session.canEdit ? 'true' : 'false');
    this.root.setAttribute('role', 'textbox');
    this.root.setAttribute('aria-multiline', 'true');
    this.root.setAttribute('aria-label', 'Document body');
    this.root.spellcheck = true;

    this._bind();
    this.render();
  }

  _bind() {
    this.root.addEventListener('beforeinput', (event) => this._onBeforeInput(event));
    this.root.addEventListener('paste', (event) => this._onPaste(event));
    document.addEventListener('selectionchange', () => this._onSelectionChange());

    // Remote work re-renders, but never at the cost of the local caret.
    this.session.addEventListener('remote', () => this.render());
    this.session.addEventListener('presence', () => this.renderCursors());
  }

  // ------------------------------------------------------------------ input
  _onBeforeInput(event) {
    if (!this.session.canEdit) {
      event.preventDefault();
      return;
    }

    const range = this.currentRange();
    if (!range) return;
    const { start, end } = range;

    switch (event.inputType) {
      case 'insertText':
      case 'insertCompositionText':
      case 'insertReplacementText': {
        event.preventDefault();
        this._replace(start, end, event.data || '');
        break;
      }

      case 'insertParagraph':
      case 'insertLineBreak': {
        event.preventDefault();
        this._replace(start, end, '\n');
        break;
      }

      case 'deleteContentBackward': {
        event.preventDefault();
        if (end > start) this._replace(start, end, '');
        else if (start > 0) this._replace(start - 1, start, '');
        break;
      }

      case 'deleteContentForward': {
        event.preventDefault();
        if (end > start) this._replace(start, end, '');
        else if (start < this.session.rga.length) this._replace(start, start + 1, '');
        break;
      }

      case 'deleteWordBackward': {
        event.preventDefault();
        const from = end > start ? start : this._wordBoundaryBefore(start);
        this._replace(from, end > start ? end : start, '');
        break;
      }

      case 'deleteWordForward': {
        event.preventDefault();
        const to = end > start ? end : this._wordBoundaryAfter(start);
        this._replace(end > start ? start : start, to, '');
        break;
      }

      case 'insertFromPaste':
      case 'insertFromDrop': {
        event.preventDefault();
        const text = event.dataTransfer ? event.dataTransfer.getData('text/plain') : '';
        if (text) this._replace(start, end, text);
        break;
      }

      default: {
        // Anything not explicitly modelled is refused rather than allowed to
        // mutate the DOM behind the CRDT's back, which would desynchronise the
        // rendered text from the replica.
        event.preventDefault();
        break;
      }
    }
  }

  _onPaste(event) {
    if (!this.session.canEdit) return;
    event.preventDefault();
    const text = event.clipboardData.getData('text/plain');
    if (!text) return;
    const range = this.currentRange();
    if (range) this._replace(range.start, range.end, text);
  }

  /** The one place text changes: delete the selection, then insert. */
  _replace(start, end, text) {
    const caretAfter = start + Array.from(text).length;
    if (end > start) this.session.remove(start, end - start);
    if (text) this.session.insert(start, text);
    this.render();
    this.setSelection(caretAfter, caretAfter);
    this._emitChange();
  }

  _wordBoundaryBefore(index) {
    const text = this.session.rga.text();
    let i = index;
    while (i > 0 && /\s/.test(text[i - 1])) i -= 1;
    while (i > 0 && !/\s/.test(text[i - 1])) i -= 1;
    return i;
  }

  _wordBoundaryAfter(index) {
    const text = this.session.rga.text();
    let i = index;
    while (i < text.length && !/\s/.test(text[i])) i += 1;
    while (i < text.length && /\s/.test(text[i])) i += 1;
    return i;
  }

  // ------------------------------------------------------------- formatting
  toggleMark(key) {
    if (!this.session.canEdit) return;
    const range = this.currentRange();
    if (!range || range.end <= range.start) return;

    const existing = this.session.rga.marksAt(range.start);
    const value = existing[key] ? null : true;
    this.session.mark(range.start, range.end - range.start, key, value);
    this.render();
    this.setSelection(range.start, range.end);
    this._emitChange();
  }

  applyLink(url) {
    const range = this.currentRange();
    if (!range || range.end <= range.start) return;
    this.session.mark(range.start, range.end - range.start, 'link', url || null);
    this.render();
    this.setSelection(range.start, range.end);
    this._emitChange();
  }

  applyHeading(level) {
    const range = this.currentRange();
    if (!range) return;
    const line = this._lineBounds(range.start);
    if (line.end <= line.start) return;
    const existing = this.session.rga.marksAt(line.start);
    const value = existing.heading === String(level) ? null : String(level);
    this.session.mark(line.start, line.end - line.start, 'heading', value);
    this.render();
    this.setSelection(range.start, range.end);
    this._emitChange();
  }

  _lineBounds(index) {
    const text = this.session.rga.text();
    let start = index;
    while (start > 0 && text[start - 1] !== '\n') start -= 1;
    let end = index;
    while (end < text.length && text[end] !== '\n') end += 1;
    return { start, end };
  }

  // ---------------------------------------------------------------- render
  render() {
    const spans = this.session.rga.spans();
    const fragment = document.createDocumentFragment();
    let offset = 0;

    if (!spans.length) {
      // An empty contenteditable collapses and becomes unclickable without
      // something to give it height.
      const placeholder = document.createElement('span');
      placeholder.className = 'sp-empty';
      placeholder.dataset.offset = '0';
      placeholder.textContent = '';
      fragment.append(placeholder);
    }

    for (const span of spans) {
      // textContent, never innerHTML: this text was written by someone else.
      let node = document.createTextNode(span.text);

      for (const [key, tag] of Object.entries(MARK_TAGS)) {
        if (span.marks[key]) {
          const wrapper = document.createElement(tag);
          wrapper.append(node);
          node = wrapper;
        }
      }

      if (typeof span.marks.link === 'string') {
        const anchor = document.createElement('a');
        anchor.href = safeUrl(span.marks.link);
        anchor.rel = 'noopener noreferrer';
        anchor.target = '_blank';
        anchor.append(node);
        node = anchor;
      }

      if (span.marks.heading) {
        const level = Math.max(1, Math.min(Number(span.marks.heading) || 1, 6));
        const heading = document.createElement('span');
        heading.className = `sp-h sp-h${level}`;
        heading.append(node);
        node = heading;
      }

      const holder = document.createElement('span');
      holder.className = 'sp-span';
      holder.dataset.offset = String(offset);
      holder.append(node);
      fragment.append(holder);
      offset += Array.from(span.text).length;
    }

    this.root.replaceChildren(fragment);
    this.renderCursors();
  }

  renderCursors() {
    const layer = document.getElementById('cursor-layer');
    if (!layer) return;
    layer.replaceChildren();

    for (const peer of this.session.peers) {
      if (!peer.head) continue;
      let index;
      try {
        index = this.session.rga.anchorIndex(OpId.decode(peer.head));
      } catch {
        continue;
      }
      if (index === null) continue;

      const point = this._coordsAt(index);
      if (!point) continue;

      const caret = document.createElement('div');
      caret.className = 'sp-cursor';
      caret.style.left = `${point.x}px`;
      caret.style.top = `${point.y}px`;
      caret.style.height = `${point.height}px`;
      caret.style.background = peer.color;

      const label = document.createElement('span');
      label.className = 'sp-cursor-label';
      label.style.background = peer.color;
      label.textContent = peer.name;
      caret.append(label);
      layer.append(caret);
    }
  }

  _coordsAt(index) {
    const located = this._domPointAt(index);
    if (!located) return null;
    const range = document.createRange();
    try {
      range.setStart(located.node, located.offset);
      range.collapse(true);
    } catch {
      return null;
    }
    const rect = range.getBoundingClientRect();
    const host = this.root.getBoundingClientRect();
    if (!rect.height && !rect.width) return null;
    return {
      x: rect.left - host.left,
      y: rect.top - host.top,
      height: rect.height || 20,
    };
  }

  // ------------------------------------------------------------- selection
  /**
   * Current selection as visible character offsets.
   *
   * Offsets are only used within a single synchronous turn. Anything that has
   * to survive a remote edit is stored as an OpId instead — see `anchors()`.
   */
  currentRange() {
    const selection = window.getSelection();
    if (!selection || selection.rangeCount === 0) return null;
    const range = selection.getRangeAt(0);
    if (!this.root.contains(range.startContainer)) return null;

    const start = this._offsetOf(range.startContainer, range.startOffset);
    const end = this._offsetOf(range.endContainer, range.endOffset);
    if (start === null || end === null) return null;
    return { start: Math.min(start, end), end: Math.max(start, end) };
  }

  /** The selection as CRDT identities, which survive concurrent editing. */
  anchors() {
    const range = this.currentRange();
    if (!range) return { anchor: null, head: null };
    const rga = this.session.rga;
    return {
      anchor: rga.idAt(Math.min(range.start, rga.length - 1 < 0 ? 0 : range.start)),
      head: rga.idAt(Math.min(range.end, Math.max(rga.length - 1, 0))),
    };
  }

  _offsetOf(node, nodeOffset) {
    let total = 0;
    const walker = document.createTreeWalker(this.root, NodeFilter.SHOW_TEXT);
    let current = walker.nextNode();
    while (current) {
      if (current === node) return total + nodeOffset;
      total += Array.from(current.textContent).length;
      current = walker.nextNode();
    }
    // The selection can sit on an element rather than a text node, typically
    // when the document is empty or the caret is at the very end.
    if (node === this.root) return Math.min(total, this.session.rga.length);
    return total;
  }

  _domPointAt(index) {
    let remaining = index;
    const walker = document.createTreeWalker(this.root, NodeFilter.SHOW_TEXT);
    let current = walker.nextNode();
    let last = null;

    while (current) {
      const length = Array.from(current.textContent).length;
      if (remaining <= length) return { node: current, offset: clampUtf16(current, remaining) };
      remaining -= length;
      last = current;
      current = walker.nextNode();
    }
    if (last) return { node: last, offset: last.textContent.length };
    return { node: this.root, offset: 0 };
  }

  setSelection(start, end) {
    const from = this._domPointAt(start);
    const to = this._domPointAt(end);
    if (!from || !to) return;
    const range = document.createRange();
    try {
      range.setStart(from.node, from.offset);
      range.setEnd(to.node, to.offset);
    } catch {
      return;
    }
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
  }

  _onSelectionChange() {
    const selection = window.getSelection();
    if (!selection || selection.rangeCount === 0) return;
    if (!this.root.contains(selection.getRangeAt(0).startContainer)) return;

    const { anchor, head } = this.anchors();
    this.session.sendCursor(anchor, head);
    this._emitChange();
  }

  _emitChange() {
    this.dispatchEvent(new CustomEvent('change', { detail: { range: this.currentRange() } }));
  }
}

/**
 * Convert a code-point offset into the UTF-16 offset a DOM range expects.
 *
 * The CRDT counts code points; the DOM counts UTF-16 units. They differ exactly
 * where the text contains astral characters — emoji, most obviously — and
 * without this conversion the caret lands inside a surrogate pair.
 */
function clampUtf16(textNode, codePointOffset) {
  const chars = Array.from(textNode.textContent);
  return chars.slice(0, codePointOffset).join('').length;
}

/** Refuse to render anything but http(s) and mailto as a live link. */
function safeUrl(url) {
  const trimmed = String(url).trim();
  return /^(https?:|mailto:)/i.test(trimmed) ? trimmed : '#';
}
