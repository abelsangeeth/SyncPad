/**
 * Application shell: routing, panels, and everything that is not the CRDT.
 *
 * Kept deliberately framework-free. The interesting parts of this project are
 * the convergence guarantee, the fan-out and the durability model; a build step
 * and a component library would add weight without adding insight, and would
 * obscure how little machinery live collaboration actually needs.
 */

import { ApiError, api, auth } from './api.js';
import { OpId } from './crdt.js';
import { Editor } from './editor.js';
import { Session } from './session.js';

const el = (id) => document.getElementById(id);

const state = {
  session: null,
  editor: null,
  docId: null,
  share: null,
  comments: [],
  user: null,
};

// ----------------------------------------------------------------- utilities

function toast(message, kind = 'info') {
  const host = el('toasts');
  const node = document.createElement('div');
  node.className = `toast toast-${kind}`;
  node.textContent = message;
  host.append(node);
  setTimeout(() => node.classList.add('toast-out'), 3200);
  setTimeout(() => node.remove(), 3700);
}

function show(view) {
  for (const name of ['auth', 'library', 'workspace']) {
    el(`view-${name}`).hidden = name !== view;
  }
}

function relativeTime(iso) {
  const then = new Date(iso).getTime();
  const seconds = Math.round((Date.now() - then) / 1000);
  if (seconds < 60) return 'just now';
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return new Date(iso).toLocaleDateString();
}

// --------------------------------------------------------------------- auth

async function handleAuthSubmit(event) {
  event.preventDefault();
  const mode = el('auth-form').dataset.mode;
  const email = el('auth-email').value.trim();
  const password = el('auth-password').value;
  const name = el('auth-name').value.trim();

  try {
    const result =
      mode === 'register'
        ? await api.register(email, name || email.split('@')[0], password)
        : await api.login(email, password);
    auth.token = result.access_token;
    state.user = result.user;
    await openLibrary();
  } catch (error) {
    const detail = error instanceof ApiError ? String(error.detail) : 'something went wrong';
    el('auth-error').textContent = detail;
    el('auth-error').hidden = false;
  }
}

function toggleAuthMode() {
  const form = el('auth-form');
  const registering = form.dataset.mode === 'register';
  form.dataset.mode = registering ? 'login' : 'register';
  el('auth-name-field').hidden = registering;
  el('auth-submit').textContent = registering ? 'Sign in' : 'Create account';
  el('auth-toggle').textContent = registering
    ? 'Need an account? Register'
    : 'Already registered? Sign in';
  el('auth-error').hidden = true;
}

function signOut() {
  auth.token = null;
  state.user = null;
  if (state.session) state.session.close();
  window.location.hash = '';
  show('auth');
}

// ------------------------------------------------------------------ library

async function openLibrary() {
  show('library');
  el('library-list').innerHTML = '<li class="muted">Loading…</li>';
  try {
    state.user = state.user || (await api.me());
    el('library-user').textContent = state.user.name;
    const documents = await api.listDocuments();
    renderLibrary(documents);
  } catch (error) {
    if (error instanceof ApiError && error.status === 401) {
      signOut();
      return;
    }
    el('library-list').innerHTML = '<li class="muted">Could not load documents.</li>';
  }
}

function renderLibrary(documents) {
  const list = el('library-list');
  list.replaceChildren();

  if (!documents.length) {
    list.innerHTML = '<li class="muted">No documents yet — create one to begin.</li>';
    return;
  }

  for (const doc of documents) {
    const item = document.createElement('li');
    item.className = 'doc-card';

    const link = document.createElement('button');
    link.className = 'doc-open';
    link.addEventListener('click', () => {
      window.location.hash = `#doc=${doc.id}`;
    });

    const title = document.createElement('span');
    title.className = 'doc-title';
    title.textContent = doc.title || 'Untitled';

    const meta = document.createElement('span');
    meta.className = 'doc-meta';
    meta.textContent = `${doc.role || 'viewer'} · edited ${relativeTime(doc.updated_at)}`;

    link.append(title, meta);
    item.append(link);

    if (doc.role === 'owner') {
      const remove = document.createElement('button');
      remove.className = 'icon-button';
      remove.title = 'Delete document';
      remove.textContent = '×';
      remove.addEventListener('click', async () => {
        if (!window.confirm(`Delete "${doc.title}"? This cannot be undone.`)) return;
        await api.deleteDocument(doc.id);
        toast('Document deleted');
        openLibrary();
      });
      item.append(remove);
    }

    list.append(item);
  }
}

async function createDocument() {
  const title = el('new-doc-title').value.trim() || 'Untitled';
  const created = await api.createDocument(title);
  el('new-doc-title').value = '';
  window.location.hash = `#doc=${created.id}`;
}

// ---------------------------------------------------------------- workspace

async function openDocument(docId, share) {
  show('workspace');
  state.docId = docId;
  state.share = share;

  if (state.session) state.session.close();

  const session = new Session({ docId, token: auth.token, share });
  state.session = session;

  session.addEventListener('ready', () => {
    el('doc-title').value = session.title;
    el('doc-title').disabled = !session.canEdit;
    el('role-badge').textContent = session.role;
    el('role-badge').dataset.role = session.role;

    if (!state.editor) {
      state.editor = new Editor(el('editor'), session);
      state.editor.addEventListener('change', updateToolbarState);
    } else {
      state.editor.session = session;
      state.editor.root.setAttribute('contenteditable', session.canEdit ? 'true' : 'false');
      state.editor.render();
    }

    el('toolbar').hidden = !session.canEdit;
    renderPeers();
    loadComments();
  });

  session.addEventListener('status', (event) => renderStatus(event.detail));
  session.addEventListener('presence', renderPeers);
  session.addEventListener('remote', () => {
    // Anchors move as text around them changes, so comment positions are
    // recomputed rather than cached.
    renderComments();
  });
  session.addEventListener('error', (event) => {
    const { code, message, fatal } = event.detail;
    toast(message, fatal ? 'error' : 'warn');
    if (code === 'auth_failed') signOut();
  });

  session.connect();
}

function renderStatus({ status, pending }) {
  const badge = el('sync-status');
  badge.dataset.status = status;
  const labels = {
    connecting: 'Connecting…',
    syncing: pending ? `Saving ${pending} change${pending === 1 ? '' : 's'}…` : 'Saving…',
    synced: 'All changes saved',
    offline: pending ? `Offline — ${pending} change${pending === 1 ? '' : 's'} queued` : 'Offline',
  };
  badge.textContent = labels[status] || status;
}

function renderPeers() {
  const host = el('peers');
  host.replaceChildren();
  const session = state.session;
  if (!session) return;

  const all = [
    { site: session.site, name: session.user ? session.user.name : 'You', color: '#1f2430', self: true },
    ...session.peers,
  ];

  for (const peer of all) {
    const avatar = document.createElement('span');
    avatar.className = 'avatar';
    avatar.style.background = peer.color;
    avatar.title = peer.self ? `${peer.name} (you)` : peer.name;
    avatar.textContent = (peer.name || '?').charAt(0).toUpperCase();
    host.append(avatar);
  }

  el('peer-count').textContent =
    session.peers.length === 0
      ? 'Only you'
      : `${session.peers.length + 1} people here`;
}

function updateToolbarState() {
  const session = state.session;
  if (!session || !state.editor) return;
  const range = state.editor.currentRange();
  const marks = range ? session.rga.marksAt(range.start) : {};
  for (const key of ['bold', 'italic', 'code', 'strike']) {
    const button = document.querySelector(`[data-mark="${key}"]`);
    if (button) button.classList.toggle('active', Boolean(marks[key]));
  }
  el('comment-add').disabled = !range || range.end <= range.start || !session.canComment;
}

// ----------------------------------------------------------------- comments

async function loadComments() {
  try {
    state.comments = await api.comments(state.docId);
  } catch {
    state.comments = [];
  }
  renderComments();
}

function renderComments() {
  const host = el('comment-list');
  host.replaceChildren();

  if (!state.comments.length) {
    host.innerHTML = '<li class="muted">No comments yet. Select text and add one.</li>';
    return;
  }

  // Recompute each anchor against the live replica so comments follow their
  // text rather than a stale offset.
  const session = state.session;
  const decorated = state.comments.map((comment) => {
    let index = null;
    let live = comment.current_text;
    if (session && session.rga) {
      try {
        const start = OpId.decode(comment.start);
        const end = OpId.decode(comment.end);
        index = session.rga.anchorIndex(start);
        live = session.rga.textBetween(start, end);
      } catch {
        index = null;
      }
    }
    return { ...comment, liveIndex: index, liveText: live, orphaned: !live };
  });

  decorated.sort((a, b) => (a.liveIndex ?? 1e9) - (b.liveIndex ?? 1e9));

  for (const comment of decorated) {
    const item = document.createElement('li');
    item.className = 'comment';
    if (comment.resolved) item.classList.add('resolved');
    if (comment.orphaned) item.classList.add('orphaned');

    const head = document.createElement('div');
    head.className = 'comment-head';

    const dot = document.createElement('span');
    dot.className = 'avatar avatar-sm';
    dot.style.background = comment.author_color;
    dot.textContent = comment.author_name.charAt(0).toUpperCase();

    const who = document.createElement('span');
    who.className = 'comment-author';
    who.textContent = comment.author_name;

    head.append(dot, who);

    const quote = document.createElement('blockquote');
    quote.className = 'comment-quote';
    quote.textContent = comment.orphaned
      ? `“${comment.quoted}” (deleted)`
      : `“${comment.liveText}”`;

    const body = document.createElement('p');
    body.className = 'comment-body';
    body.textContent = comment.body;

    const actions = document.createElement('div');
    actions.className = 'comment-actions';

    if (comment.liveIndex !== null && !comment.orphaned) {
      const jump = document.createElement('button');
      jump.className = 'link-button';
      jump.textContent = 'Jump to';
      jump.addEventListener('click', () => {
        state.editor.setSelection(comment.liveIndex, comment.liveIndex + comment.liveText.length);
        el('editor').focus();
      });
      actions.append(jump);
    }

    const resolve = document.createElement('button');
    resolve.className = 'link-button';
    resolve.textContent = comment.resolved ? 'Reopen' : 'Resolve';
    resolve.addEventListener('click', async () => {
      await api.resolveComment(state.docId, comment.id);
      await loadComments();
    });
    actions.append(resolve);

    if (state.user && comment.author_id === state.user.id) {
      const remove = document.createElement('button');
      remove.className = 'link-button danger';
      remove.textContent = 'Delete';
      remove.addEventListener('click', async () => {
        await api.deleteComment(state.docId, comment.id);
        await loadComments();
      });
      actions.append(remove);
    }

    item.append(head, quote, body, actions);
    host.append(item);
  }
}

async function addComment() {
  const range = state.editor.currentRange();
  if (!range || range.end <= range.start) return;

  const body = window.prompt('Comment on the selected text:');
  if (!body) return;

  const rga = state.session.rga;
  const start = rga.idAt(range.start);
  const end = rga.idAt(range.end - 1);
  if (!start || !end) return;

  try {
    await api.addComment(state.docId, start.encode(), end.encode(), body);
    await loadComments();
    toast('Comment added');
  } catch (error) {
    toast(error instanceof ApiError ? String(error.detail) : 'Could not add comment', 'error');
  }
}

// ------------------------------------------------------------------ history

async function openHistory() {
  const panel = el('history-panel');
  panel.hidden = false;
  const list = el('history-list');
  list.innerHTML = '<li class="muted">Loading…</li>';

  const data = await api.history(state.docId);
  list.replaceChildren();

  if (!data.revisions.length) {
    list.innerHTML = '<li class="muted">No revisions yet.</li>';
    return;
  }

  for (const revision of data.revisions) {
    const item = document.createElement('li');
    item.className = 'revision';
    const reachable = revision.seq >= data.earliest_reachable;

    const label = document.createElement('div');
    label.className = 'revision-label';
    label.textContent = `#${revision.seq} · ${revision.author_name || 'Unknown'} · ${relativeTime(revision.created_at)}`;
    if (revision.is_snapshot) {
      const badge = document.createElement('span');
      badge.className = 'chip';
      badge.textContent = 'snapshot';
      label.append(badge);
    }

    const actions = document.createElement('div');
    actions.className = 'revision-actions';

    if (!reachable) {
      const gone = document.createElement('span');
      gone.className = 'muted';
      gone.textContent = 'compacted';
      actions.append(gone);
    } else {
      const preview = document.createElement('button');
      preview.className = 'link-button';
      preview.textContent = 'Preview';
      preview.addEventListener('click', async () => {
        const version = await api.versionAt(state.docId, revision.seq);
        el('history-preview').textContent = version.text || '(empty)';
        el('history-preview').hidden = false;
      });

      const restore = document.createElement('button');
      restore.className = 'link-button';
      restore.textContent = 'Restore';
      restore.disabled = !state.session.canEdit;
      restore.addEventListener('click', async () => {
        if (!window.confirm(`Restore revision #${revision.seq}? This is recorded as a new edit.`)) {
          return;
        }
        await api.restore(state.docId, revision.seq);
        toast(`Restored revision #${revision.seq}`);
        panel.hidden = true;
        // The restore arrives over the socket as ordinary operations.
      });

      actions.append(preview, restore);
    }

    item.append(label, actions);
    list.append(item);
  }
}

// -------------------------------------------------------------------- share

async function openShare() {
  const panel = el('share-panel');
  panel.hidden = false;
  const doc = await api.getDocument(state.docId);
  el('share-public').checked = doc.is_public;

  const list = el('share-list');
  list.replaceChildren();
  try {
    const people = await api.collaborators(state.docId);
    for (const entry of people) {
      const item = document.createElement('li');
      item.textContent = `${entry.user.name} (${entry.user.email}) — ${entry.role}`;
      list.append(item);
    }
    if (!people.length) list.innerHTML = '<li class="muted">No collaborators yet.</li>';
  } catch {
    list.innerHTML = '<li class="muted">Only the owner can see collaborators.</li>';
  }
}

async function createShareLink() {
  const role = el('share-role').value;
  const link = await api.share(state.docId, role, null);
  const url = `${window.location.origin}/${link.url_path.replace(/^\//, '')}`;
  el('share-link').value = url;
  el('share-link').hidden = false;
  try {
    await navigator.clipboard.writeText(url);
    toast('Share link copied to clipboard');
  } catch {
    toast('Share link created');
  }
}

async function inviteCollaborator() {
  const email = el('share-email').value.trim();
  if (!email) return;
  try {
    await api.grant(state.docId, email, el('share-invite-role').value);
    el('share-email').value = '';
    toast(`Invited ${email}`);
    openShare();
  } catch (error) {
    toast(error instanceof ApiError ? String(error.detail) : 'Could not invite', 'error');
  }
}

// ------------------------------------------------------------------ routing

function route() {
  const hash = new URLSearchParams(window.location.hash.replace(/^#/, ''));
  const docId = hash.get('doc');
  const share = hash.get('share');

  if (docId) {
    openDocument(docId, share);
  } else if (auth.isSignedIn) {
    openLibrary();
  } else {
    show('auth');
  }
}

function bindChrome() {
  el('auth-form').addEventListener('submit', handleAuthSubmit);
  el('auth-toggle').addEventListener('click', toggleAuthMode);
  el('sign-out').addEventListener('click', signOut);
  el('library-sign-out').addEventListener('click', signOut);

  el('new-doc-form').addEventListener('submit', (event) => {
    event.preventDefault();
    createDocument();
  });

  el('back-to-library').addEventListener('click', () => {
    window.location.hash = '';
  });

  el('doc-title').addEventListener('change', async (event) => {
    await api.renameDocument(state.docId, event.target.value.trim() || 'Untitled');
    toast('Renamed');
  });

  for (const button of document.querySelectorAll('[data-mark]')) {
    button.addEventListener('mousedown', (event) => {
      // Keep focus in the editor so the selection survives the click.
      event.preventDefault();
      state.editor.toggleMark(button.dataset.mark);
    });
  }

  for (const button of document.querySelectorAll('[data-heading]')) {
    button.addEventListener('mousedown', (event) => {
      event.preventDefault();
      state.editor.applyHeading(Number(button.dataset.heading));
    });
  }

  el('link-button').addEventListener('mousedown', (event) => {
    event.preventDefault();
    const url = window.prompt('Link URL (leave empty to remove):', 'https://');
    if (url === null) return;
    state.editor.applyLink(url.trim() || null);
  });

  el('comment-add').addEventListener('click', addComment);
  el('history-open').addEventListener('click', openHistory);
  el('history-close').addEventListener('click', () => {
    el('history-panel').hidden = true;
    el('history-preview').hidden = true;
  });
  el('share-open').addEventListener('click', openShare);
  el('share-close').addEventListener('click', () => {
    el('share-panel').hidden = true;
  });
  el('share-create').addEventListener('click', createShareLink);
  el('share-invite').addEventListener('click', inviteCollaborator);
  el('share-public').addEventListener('change', async (event) => {
    await api.setPublic(state.docId, event.target.checked);
    toast(event.target.checked ? 'Anyone with the link can now read this' : 'Document is private');
  });

  for (const button of document.querySelectorAll('[data-export]')) {
    button.addEventListener('click', () => {
      window.location.href = api.exportUrl(state.docId, button.dataset.export);
    });
  }

  // Keyboard shortcuts, matching what every other editor uses.
  document.addEventListener('keydown', (event) => {
    if (!state.editor || !(event.metaKey || event.ctrlKey)) return;
    const shortcuts = { b: 'bold', i: 'italic', e: 'code' };
    const mark = shortcuts[event.key.toLowerCase()];
    if (mark) {
      event.preventDefault();
      state.editor.toggleMark(mark);
    }
  });

  window.addEventListener('hashchange', route);
}

bindChrome();
route();
