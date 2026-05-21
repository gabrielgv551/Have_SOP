import { useState, useEffect, useCallback } from 'react'
import { api } from '../lib/api'
import { Building2, RefreshCw, Wifi, WifiOff, ChevronLeft, ChevronRight, TrendingUp, TrendingDown } from 'lucide-react'

const MESES = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez']

function fmt(centavos) {
  return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format((centavos || 0) / 100)
}

function fmtDoc(v) {
  const d = (v || '').replace(/\D/g, '')
  if (d.length === 14) return d.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, '$1.$2.$3/$4-$5')
  if (d.length === 11) return d.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, '$1.$2.$3-$4')
  return v || null
}

// ─── ABA BANCOS (Open Finance) ────────────────────────────────────
function TabBancos() {
  const [links, setLinks]   = useState([])
  const [loading, setLoading] = useState(true)
  const [syncing, setSyncing] = useState(null)
  const [msg, setMsg]       = useState('')

  const today = new Date().toISOString().split('T')[0]
  const [dates, setDates] = useState({})

  const fetchLinks = useCallback(async () => {
    setLoading(true)
    try {
      const d = await api.get('/api/caixa-extrato?module=pluggy')
      setLinks(d.links || [])
      const init = {}
      ;(d.links || []).forEach(l => {
        init[l.link_id] = { from: '2026-01-01', to: today }
      })
      setDates(init)
    } catch (e) { setMsg(e.message) }
    setLoading(false)
  }, [today])

  useEffect(() => { fetchLinks() }, [fetchLinks])

  const sync = async (link_id) => {
    const { from, to } = dates[link_id] || { from: '2026-01-01', to: today }
    setSyncing(link_id)
    setMsg('')
    try {
      const d = await api.post('/api/caixa-extrato?module=pluggy', { action: 'sync', link_id, date_from: from, date_to: to })
      setMsg(`✅ ${d.count} transações importadas`)
      fetchLinks()
    } catch (e) { setMsg(`❌ ${e.message}`) }
    setSyncing(null)
  }

  const setDate = (link_id, field, val) =>
    setDates(prev => ({ ...prev, [link_id]: { ...(prev[link_id] || {}), [field]: val } }))

  if (loading) return <p className="text-sm text-gray-400 p-6">Carregando...</p>

  return (
    <div className="space-y-4">
      <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 text-sm text-blue-800">
        <strong>Open Finance via Pluggy</strong> — Conecte as contas bancárias da empresa para importar extratos automaticamente para o Fluxo de Caixa.<br />
        Os lançamentos serão salvos na Base de Dados e classificados pelo De-Para.
      </div>

      {msg && (
        <div className={`rounded-xl px-4 py-3 text-sm font-medium ${msg.startsWith('✅') ? 'bg-green-50 text-green-700 border border-green-200' : 'bg-red-50 text-red-700 border border-red-200'}`}>
          {msg}
        </div>
      )}

      {links.length === 0 ? (
        <div className="bg-white rounded-xl border border-gray-200 p-10 text-center">
          <WifiOff className="w-10 h-10 text-gray-300 mx-auto mb-3" />
          <p className="text-gray-500 font-medium">Nenhum banco conectado</p>
          <p className="text-gray-400 text-sm mt-1">Conecte um banco pelo Extrator Bancários ou pelo botão abaixo.</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {links.map(l => (
            <div key={l.link_id} className="bg-white rounded-xl border border-gray-200 p-5 shadow-sm space-y-3">
              <div className="flex items-center gap-2">
                <Building2 className="w-5 h-5 text-blue-500 shrink-0" />
                <span className="font-semibold text-gray-900 text-sm flex-1 truncate">{l.institution || l.link_id.slice(0,12)+'...'}</span>
                <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${l.ativo ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500'}`}>
                  {l.ativo ? 'Ativo' : 'Inativo'}
                </span>
              </div>
              {l.ultimo_sync && (
                <p className="text-xs text-gray-400">Última sync: {new Date(l.ultimo_sync).toLocaleString('pt-BR')}</p>
              )}
              <p className="text-xs text-gray-400 font-mono break-all">{l.link_id}</p>
              <div className="flex gap-2">
                <div>
                  <label className="text-xs text-gray-400">De</label>
                  <input type="date" value={dates[l.link_id]?.from || '2026-01-01'}
                    onChange={e => setDate(l.link_id, 'from', e.target.value)}
                    className="block mt-0.5 border border-gray-200 rounded-lg px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400" />
                </div>
                <div>
                  <label className="text-xs text-gray-400">Até</label>
                  <input type="date" value={dates[l.link_id]?.to || today}
                    onChange={e => setDate(l.link_id, 'to', e.target.value)}
                    className="block mt-0.5 border border-gray-200 rounded-lg px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400" />
                </div>
              </div>
              <div className="flex gap-2">
                <button onClick={() => sync(l.link_id)} disabled={!!syncing}
                  className="flex-1 flex items-center justify-center gap-1.5 bg-blue-600 text-white text-xs px-3 py-2 rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors font-medium">
                  <RefreshCw className={`w-3.5 h-3.5 ${syncing === l.link_id ? 'animate-spin' : ''}`} />
                  {syncing === l.link_id ? 'Sincronizando...' : 'Sincronizar'}
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ─── ABA EXTRATOS ─────────────────────────────────────────────────
function TabExtratos() {
  const now = new Date()
  const [ano, setAno]     = useState(now.getFullYear())
  const [mes, setMes]     = useState(now.getMonth() + 1)
  const [bancos, setBancos]       = useState([])
  const [bancoId, setBancoId]     = useState('')
  const [rows, setRows]           = useState([])
  const [mesesComDados, setMesesComDados] = useState([])
  const [loading, setLoading]     = useState(false)
  const [loadingMeses, setLoadingMeses] = useState(true)

  useEffect(() => {
    api.get('/api/caixa-extrato?module=bancos')
      .then(d => setBancos(d.bancos || []))
      .catch(() => {})
    api.get('/api/caixa-extrato')
      .then(d => { setMesesComDados(d.meses || []); setLoadingMeses(false) })
      .catch(() => setLoadingMeses(false))
  }, [])

  const buscar = useCallback(async () => {
    setLoading(true)
    try {
      const qs = bancoId ? `&banco_id=${bancoId}` : ''
      const d = await api.get(`/api/caixa-extrato?ano=${ano}&mes=${mes}${qs}`)
      setRows(d.rows || [])
    } catch (e) { setRows([]) }
    setLoading(false)
  }, [ano, mes, bancoId])

  useEffect(() => { buscar() }, [buscar])

  const navMes = (dir) => {
    let m = mes + dir, a = ano
    if (m < 1) { m = 12; a-- }
    if (m > 12) { m = 1; a++ }
    setMes(m); setAno(a)
  }

  const temDados = (a, m) => mesesComDados.some(x => x.ano === a && x.mes === m)

  const entradas = rows.filter(r => r.valor > 0).reduce((s, r) => s + r.valor, 0)
  const saidas   = rows.filter(r => r.valor < 0).reduce((s, r) => s + r.valor, 0)

  return (
    <div className="space-y-4">
      {/* Filtros */}
      <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm flex flex-wrap items-end gap-4">
        <div className="flex items-center gap-2">
          <button onClick={() => navMes(-1)} className="p-1.5 rounded-lg hover:bg-gray-100 transition-colors">
            <ChevronLeft className="w-4 h-4 text-gray-500" />
          </button>
          <div className="flex gap-2">
            <select value={mes} onChange={e => setMes(+e.target.value)}
              className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-1 focus:ring-blue-400">
              {MESES.map((m, i) => (
                <option key={i+1} value={i+1}>
                  {m} {temDados(ano, i+1) ? '●' : ''}
                </option>
              ))}
            </select>
            <input type="number" value={ano} onChange={e => setAno(+e.target.value)}
              className="w-20 border border-gray-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-1 focus:ring-blue-400" />
          </div>
          <button onClick={() => navMes(1)} className="p-1.5 rounded-lg hover:bg-gray-100 transition-colors">
            <ChevronRight className="w-4 h-4 text-gray-500" />
          </button>
        </div>
        <div>
          <label className="block text-xs text-gray-400 mb-1">Banco</label>
          <select value={bancoId} onChange={e => setBancoId(e.target.value)}
            className="border border-gray-200 rounded-lg px-3 py-1.5 text-sm focus:outline-none focus:ring-1 focus:ring-blue-400">
            <option value="">Todos os bancos</option>
            {bancos.map(b => <option key={b.id} value={b.id}>{b.nome}</option>)}
          </select>
        </div>
        {loading && <RefreshCw className="w-4 h-4 text-blue-400 animate-spin" />}
      </div>

      {/* Resumo */}
      {rows.length > 0 && (
        <div className="grid grid-cols-3 gap-4">
          {[
            { label: 'Entradas', valor: entradas, Icon: TrendingUp,   bg: 'bg-green-50', fg: 'text-green-600', txt: 'text-green-700' },
            { label: 'Saídas',   valor: saidas,   Icon: TrendingDown, bg: 'bg-red-50',   fg: 'text-red-600',   txt: 'text-red-700' },
            { label: 'Saldo',    valor: entradas + saidas, Icon: Building2,
              bg: entradas + saidas >= 0 ? 'bg-blue-50' : 'bg-orange-50',
              fg: entradas + saidas >= 0 ? 'text-blue-600' : 'text-orange-600',
              txt: entradas + saidas >= 0 ? 'text-blue-700' : 'text-orange-700' },
          ].map(({ label, valor, Icon, bg, fg, txt }) => (
            <div key={label} className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
              <div className={`w-8 h-8 ${bg} rounded-lg flex items-center justify-center mb-2`}>
                <Icon className={`w-4 h-4 ${fg}`} />
              </div>
              <p className={`text-lg font-bold ${txt}`}>{fmt(Math.abs(valor))}</p>
              <p className="text-xs text-gray-400 mt-0.5">{label}</p>
            </div>
          ))}
        </div>
      )}

      {/* Tabela */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-100 flex items-center justify-between">
          <p className="text-sm font-semibold text-gray-700">
            {MESES[mes-1]} {ano} — {rows.length} lançamentos
          </p>
        </div>
        {rows.length === 0 ? (
          <div className="p-10 text-center">
            <RefreshCw className="w-8 h-8 text-gray-200 mx-auto mb-3" />
            <p className="text-gray-400 text-sm">Nenhum lançamento neste período</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100 bg-gray-50 text-left">
                  <th className="px-4 py-2.5 text-xs font-semibold text-gray-500 w-12">Dia</th>
                  <th className="px-4 py-2.5 text-xs font-semibold text-gray-500">Descrição</th>
                  <th className="px-4 py-2.5 text-xs font-semibold text-gray-500">Razão Social</th>
                  <th className="px-4 py-2.5 text-xs font-semibold text-gray-500 whitespace-nowrap">CNPJ</th>
                  <th className="px-4 py-2.5 text-xs font-semibold text-gray-500">Banco</th>
                  <th className="px-4 py-2.5 text-xs font-semibold text-gray-500 text-right whitespace-nowrap">Valor</th>
                  <th className="px-4 py-2.5 text-xs font-semibold text-gray-500">Categorização</th>
                </tr>
              </thead>
              <tbody>
                {rows.map(r => (
                  <tr key={r.id} className="border-b border-gray-50 hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-2 text-gray-500 text-xs">{String(r.dia).padStart(2,'0')}</td>
                    <td className="px-4 py-2 text-gray-900 max-w-xs">
                      <span className="block truncate" title={r.descricao}>{r.descricao}</span>
                    </td>
                    <td className="px-4 py-2 text-gray-500 text-xs max-w-[180px]">
                      <span className="block truncate">{r.razao_social || '—'}</span>
                    </td>
                    <td className="px-4 py-2 text-gray-500 text-xs whitespace-nowrap font-mono">
                      {fmtDoc(r.counterparty_document) || '—'}
                    </td>
                    <td className="px-4 py-2 text-gray-500 text-xs whitespace-nowrap">
                      {r.banco_nome || '—'}
                    </td>
                    <td className={`px-4 py-2 text-right font-semibold whitespace-nowrap ${r.valor >= 0 ? 'text-green-700' : 'text-red-700'}`}>
                      {r.valor >= 0 ? '+' : ''}{fmt(r.valor)}
                    </td>
                    <td className="px-4 py-2 text-xs">
                      {r.categoria
                        ? <span className="inline-block px-2 py-0.5 rounded-full bg-blue-50 text-blue-700 font-medium border border-blue-100">{r.categoria}</span>
                        : <span className="text-gray-300">—</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

// ─── PÁGINA PRINCIPAL ─────────────────────────────────────────────
export default function Caixa() {
  const [tab, setTab] = useState('extratos')

  return (
    <div className="p-6 space-y-6 max-w-6xl mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-[#1c2d4a]">Caixa · Open Finance</h1>
          <p className="text-xs text-gray-400 mt-0.5">
            {new Date().toLocaleDateString('pt-BR', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' })}
          </p>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 bg-gray-100 rounded-xl p-1 w-fit">
        {[
          { key: 'bancos',   label: 'Bancos' },
          { key: 'extratos', label: 'Extratos' },
        ].map(t => (
          <button key={t.key} onClick={() => setTab(t.key)}
            className={`px-5 py-2 rounded-lg text-sm font-medium transition-colors ${
              tab === t.key ? 'bg-white text-[#1c2d4a] shadow-sm' : 'text-gray-500 hover:text-gray-700'
            }`}>
            {t.label}
          </button>
        ))}
      </div>

      {/* Conteúdo */}
      {tab === 'bancos'   && <TabBancos />}
      {tab === 'extratos' && <TabExtratos />}
    </div>
  )
}
