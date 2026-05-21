import { createContext, useContext, useState, useCallback } from 'react'
import { api } from '../lib/api'

const AuthContext = createContext(null)

export function AuthProvider({ children }) {
  const [user, setUser] = useState(() => {
    try { return JSON.parse(localStorage.getItem('hg_user')) } catch { return null }
  })

  const login = useCallback(async (email, password, company) => {
    const data = await api.login(email, password, company)
    localStorage.setItem('hg_token', data.token)
    localStorage.setItem('hg_user', JSON.stringify(data.user || { email, company }))
    setUser(data.user || { email, company })
    return data
  }, [])

  const logout = useCallback(() => {
    localStorage.removeItem('hg_token')
    localStorage.removeItem('hg_user')
    setUser(null)
  }, [])

  return (
    <AuthContext.Provider value={{ user, login, logout, isAuthenticated: !!user }}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  return useContext(AuthContext)
}
