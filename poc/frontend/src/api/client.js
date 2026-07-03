const BASE = '/api/v1'

async function request(path, options = {}) {
  const res = await fetch(BASE + path, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined
  })
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`)
  if (res.status === 204) return null
  return res.json()
}

export const api = {
  // Rules
  getRules: () => request('/rules'),
  createRule: (data) => request('/rules', { method: 'POST', body: data }),
  updateRule: (id, data) => request(`/rules/${id}`, { method: 'PATCH', body: data }),
  deleteRule: (id) => request(`/rules/${id}`, { method: 'DELETE' }),

  // Alerts
  getAlerts: (params = {}) => {
    const q = new URLSearchParams(Object.entries(params).filter(([, v]) => v != null)).toString()
    return request(`/alerts${q ? '?' + q : ''}`)
  },
  acknowledgeAlert: (id) => request(`/alerts/${id}/acknowledge`, { method: 'POST' }),
  resolveAlert: (id) => request(`/alerts/${id}/resolve`, { method: 'POST' }),

  // Events
  getEvents: (params = {}) => {
    const q = new URLSearchParams(Object.entries(params).filter(([, v]) => v != null)).toString()
    return request(`/events${q ? '?' + q : ''}`)
  },

  // Event sources
  getEventSources: () => request('/event-sources'),

  // Authoring
  generateCel: (sourceId, description) =>
    request('/authoring/generate-cel', { method: 'POST', body: { sourceId, description } })
}
