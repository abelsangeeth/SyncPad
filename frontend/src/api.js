/**
 * The REST surface.
 *
 * Everything realtime goes over the WebSocket; this covers the rest — accounts,
 * the document list, sharing, version history and comments. The token lives in
 * localStorage, which is a considered tradeoff rather than an oversight: a
 * same-origin XSS would be able to read it, but the alternative (a cookie) buys
 * little without a backend session store and costs the ability to open a
 * document from a share link in a fresh tab.
 */

const TOKEN_KEY = 'syncpad.token';

export const auth = {
  get token() {
    return localStorage.getItem(TOKEN_KEY);
  },
  set token(value) {
    if (value) localStorage.setItem(TOKEN_KEY, value);
    else localStorage.removeItem(TOKEN_KEY);
  },
  get isSignedIn() {
    return Boolean(this.token);
  },
};

export class ApiError extends Error {
  constructor(status, detail) {
    super(typeof detail === 'string' ? detail : 'request failed');
    this.status = status;
    this.detail = detail;
  }
}

async function request(method, path, body) {
  const headers = { 'Content-Type': 'application/json' };
  if (auth.token) headers.Authorization = `Bearer ${auth.token}`;

  const response = await fetch(path, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  if (response.status === 204) return null;

  const text = await response.text();
  let payload = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = text;
    }
  }

  if (!response.ok) {
    const detail = payload && payload.detail ? payload.detail : payload;
    throw new ApiError(response.status, detail);
  }
  return payload;
}

export const api = {
  get: (path) => request('GET', path),
  post: (path, body) => request('POST', path, body),
  patch: (path, body) => request('PATCH', path, body),
  put: (path, body) => request('PUT', path, body),
  del: (path) => request('DELETE', path),

  // --- accounts ---
  register: (email, name, password) =>
    request('POST', '/api/auth/register', { email, name, password }),
  login: (email, password) => request('POST', '/api/auth/login', { email, password }),
  me: () => request('GET', '/api/auth/me'),

  // --- documents ---
  listDocuments: () => request('GET', '/api/documents'),
  createDocument: (title, text = '') => request('POST', '/api/documents', { title, text }),
  getDocument: (id) => request('GET', `/api/documents/${id}`),
  renameDocument: (id, title) => request('PATCH', `/api/documents/${id}`, { title }),
  deleteDocument: (id) => request('DELETE', `/api/documents/${id}`),
  setPublic: (id, isPublic) => request('PUT', `/api/documents/${id}/public`, { is_public: isPublic }),
  share: (id, role, hours) =>
    request('POST', `/api/documents/${id}/share`, { role, expires_in_hours: hours || null }),
  collaborators: (id) => request('GET', `/api/documents/${id}/collaborators`),
  grant: (id, email, role) =>
    request('POST', `/api/documents/${id}/collaborators`, { email, role }),
  revoke: (id, userId) => request('DELETE', `/api/documents/${id}/collaborators/${userId}`),
  exportUrl: (id, format) => `/api/documents/${id}/export?format=${format}`,

  // --- history ---
  history: (id) => request('GET', `/api/documents/${id}/history`),
  versionAt: (id, seq) => request('GET', `/api/documents/${id}/history/at/${seq}`),
  restore: (id, seq) => request('POST', `/api/documents/${id}/history/restore/${seq}`),

  // --- comments ---
  comments: (id) => request('GET', `/api/documents/${id}/comments`),
  addComment: (id, start, end, body, parentId = null) =>
    request('POST', `/api/documents/${id}/comments`, {
      start,
      end,
      body,
      parent_id: parentId,
    }),
  resolveComment: (id, commentId) =>
    request('POST', `/api/documents/${id}/comments/${commentId}/resolve`),
  deleteComment: (id, commentId) => request('DELETE', `/api/documents/${id}/comments/${commentId}`),
};
