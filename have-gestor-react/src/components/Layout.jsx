import { Outlet, NavLink, useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext'
import {
  LayoutDashboard, Wallet, Package, Settings, LogOut, ChevronRight, Menu, X
} from 'lucide-react'
import { useState } from 'react'

const NAV = [
  { to: '/dashboard', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/caixa',     icon: Wallet,          label: 'Fluxo de Caixa' },
  { to: '/estoque',   icon: Package,         label: 'Estoque' },
  { to: '/admin',     icon: Settings,        label: 'Admin' },
]

export default function Layout() {
  const { user, logout } = useAuth()
  const navigate = useNavigate()
  const [open, setOpen] = useState(true)

  function handleLogout() {
    logout()
    navigate('/login')
  }

  return (
    <div className="flex h-screen overflow-hidden bg-[#f4f7fb]">
      {/* Sidebar */}
      <aside className={`${open ? 'w-56' : 'w-16'} transition-all duration-200 flex flex-col bg-[#1c2d4a] text-white shrink-0`}>
        {/* Logo */}
        <div className="flex items-center gap-3 px-4 py-5 border-b border-[#2a3d5e]">
          <div className="w-8 h-8 rounded-lg bg-[#007CDC] flex items-center justify-center font-bold text-sm shrink-0">H</div>
          {open && <span className="font-semibold text-sm tracking-wide">Have Gestor</span>}
        </div>

        {/* Nav */}
        <nav className="flex-1 py-4 space-y-1 px-2">
          {NAV.map(({ to, icon: Icon, label }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) =>
                `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-colors
                 ${isActive ? 'bg-[#007CDC] text-white' : 'text-[#94a3b8] hover:bg-[#2a3d5e] hover:text-white'}`
              }
            >
              <Icon size={18} className="shrink-0" />
              {open && <span>{label}</span>}
            </NavLink>
          ))}
        </nav>

        {/* User + toggle */}
        <div className="border-t border-[#2a3d5e] p-3 space-y-2">
          {open && (
            <div className="px-2 py-1">
              <p className="text-xs text-[#94a3b8] truncate">{user?.usuario || user?.email}</p>
              <p className="text-xs text-[#64748b] capitalize">{user?.empresa || user?.company}</p>
            </div>
          )}
          <button
            onClick={handleLogout}
            className="flex items-center gap-3 w-full px-3 py-2 rounded-lg text-sm text-[#94a3b8] hover:bg-[#2a3d5e] hover:text-white transition-colors"
          >
            <LogOut size={16} className="shrink-0" />
            {open && <span>Sair</span>}
          </button>
          <button
            onClick={() => setOpen(o => !o)}
            className="flex items-center gap-3 w-full px-3 py-2 rounded-lg text-sm text-[#64748b] hover:bg-[#2a3d5e] transition-colors"
          >
            {open ? <X size={16} /> : <Menu size={16} />}
          </button>
        </div>
      </aside>

      {/* Main */}
      <main className="flex-1 overflow-y-auto">
        <Outlet />
      </main>
    </div>
  )
}
