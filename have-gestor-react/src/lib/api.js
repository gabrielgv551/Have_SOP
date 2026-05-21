const BASE = import.meta.env.VITE_API_URL || 'https://have-gestor-api.vercel.app'

function getToken() {
  return localStorage.getItem('hg_token')
}

async function request(path, options = {}) {
  const token = getToken()
  const res = await fetch(`${BASE}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(options.headers || {}),
    },
  })

  if (res.status === 401) {
    localStorage.removeItem('hg_token')
    localStorage.removeItem('hg_user')
    window.location.href = '/login'
    return
  }

  const data = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(data.error || `Erro ${res.status}`)
  return data
}

export const api = {
  get:    (path)         => request(path),
  post:   (path, body)   => request(path, { method: 'POST',   body: JSON.stringify(body) }),
  put:    (path, body)   => request(path, { method: 'PUT',    body: JSON.stringify(body) }),
  delete: (path)         => request(path, { method: 'DELETE' }),

  login: (email, password, company) =>
    request('/api/login', { method: 'POST', body: JSON.stringify({ email, password, company }) }),

  data: (company, params) => {
    const qs = new URLSearchParams(params).toString()
    return request(`/api/data?company=${company}&${qs}`)
  },
}
