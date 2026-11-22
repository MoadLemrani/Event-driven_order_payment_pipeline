import React, { useCallback, useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'
import { Activity, ArrowUpRight, Boxes, CircleAlert, Clock3, Layers3, RefreshCw, Search, ShieldCheck, SlidersHorizontal, X } from 'lucide-react'
import './style.css'

type Order = { id: string; sku: string; quantity: number; totalAmount: number; status: string; correlationId: string; createdAt: string }
type Event = { eventId: string; eventType: string; occurredAt: string; correlationId: string }
type DeadLetter = { id: number; envelope: string | null; errorMessage: string | null; receivedAt: string; replayRequestedAt: string | null; messageKey: string | null }
type Item = DeadLetter & { service: string }
const services = ['order', 'inventory', 'payment'] as const
async function loadDeadLetters(service: string): Promise<Item[]> {
  const all: Item[] = []; let after = 0
  for (let page = 0; page < 10; page++) {
    const batch = await api<DeadLetter[]>(`/api/${service}/dead-letters?after=${after}`)
    all.push(...batch.map(item => ({...item, service})))
    if (batch.length < 100) break
    after = batch[batch.length - 1].id
  }
  return all
}
const formatDate = (s: string) => new Date(s).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' })
async function api<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init)
  if (!response.ok) { const body = await response.text(); throw new Error(`${response.status} ${body || response.statusText}`) }
  if (response.status === 202) return undefined as T
  return response.json() as Promise<T>
}
function App() {
  const [orders, setOrders] = useState<Order[]>([]), [selected, setSelected] = useState<string | null>(null)
  const [events, setEvents] = useState<Event[]>([]), [dead, setDead] = useState<Item[]>([])
  const [page, setPage] = useState<'orders' | 'dead'>('orders'), [query, setQuery] = useState('')
  const [rate, setRate] = useState<number | null>(null), [busy, setBusy] = useState(false), [error, setError] = useState('')
  const [loading, setLoading] = useState(true), [updated, setUpdated] = useState<Date | null>(null)
  const refresh = useCallback(async () => {
    const results = await Promise.allSettled([
      api<Order[]>('/api/order/orders'), api<{failureRate:number}>('/api/payment/payment-control'),
      ...services.map(loadDeadLetters)
    ])
    if (results[0].status === 'fulfilled') setOrders(results[0].value as Order[])
    if (results[1].status === 'fulfilled') setRate((results[1].value as {failureRate:number}).failureRate)
    const dlq = results.slice(2)
    if (dlq.every(r => r.status === 'fulfilled')) setDead(dlq.flatMap(r => (r as PromiseFulfilledResult<Item[]>).value).sort((a,b) => b.receivedAt.localeCompare(a.receivedAt)))
    const failures = results.filter(r => r.status === 'rejected')
    setError(failures.length ? 'Some services are unavailable. Showing the latest available data.' : '')
    setLoading(false); setUpdated(new Date())
  }, [])
  useEffect(() => { void refresh(); const timer = setInterval(() => void refresh(), 3000); return () => clearInterval(timer) }, [refresh])
  useEffect(() => {
    if (!selected) { setEvents([]); return }
    let active = true
    const load = () => api<Event[]>(`/api/order/orders/${selected}/timeline`).then(v => {if(active) setEvents(v)}).catch(() => {if(active) setError('Could not load this order timeline.')})
    void load(); const timer = setInterval(load, 2000); return () => {active = false; clearInterval(timer)}
  }, [selected])
  const mutate = async (f: () => Promise<unknown>) => { setBusy(true); try { await f(); await refresh() } catch(e) { setError(e instanceof Error ? e.message : 'Request failed') } finally {setBusy(false)} }
  const create = () => mutate(async () => { const order = await api<Order>('/api/order/orders', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sku:'DEMO-SKU',quantity:1,totalAmount:19.99})}); setSelected(order.id); setPage('orders') })
  const filtered = orders.filter(o => `${o.id} ${o.sku} ${o.status}`.toLowerCase().includes(query.toLowerCase()))
  const current = orders.find(o => o.id === selected)
  const pending = dead.filter(d => !d.replayRequestedAt).length
  return <div className="app"><aside className="sidebar"><div className="brand"><span className="brand-icon"><Layers3 size={20}/></span><div><strong>relay</strong><small>ORDER PIPELINE</small></div></div><div className="nav-label">WORKSPACE</div><nav><button className={page==='orders'?'active':''} onClick={()=>setPage('orders')}><Activity size={18}/> Orders</button><button className={page==='dead'?'active':''} onClick={()=>setPage('dead')}><CircleAlert size={18}/> Dead letters {pending>0 && <span className="nav-count">{pending}</span>}</button></nav><div className="sidebar-bottom"><div className="sidebar-box"><ShieldCheck size={17}/><div><strong>Pipeline observability</strong><p>Track every event from order to outcome.</p></div></div><span>PORTFOLIO PROJECT <b>v1.0</b></span></div></aside>
    <main><header className="topbar"><div><span className="breadcrumb">Workspace</span><span className="slash">/</span><strong>{page==='orders'?'Orders':'Dead letters'}</strong></div><div className="top-right"><span className="live"><i/> {error ? 'Partial connection' : 'Live polling'}</span><button className="icon-button" aria-label="Refresh" onClick={()=>void refresh()}><RefreshCw size={17}/></button><div className="avatar">R</div></div></header>
      <div className="content"><div className="heading"><div><div className="eyebrow">OPERATIONS <span>/</span> {page==='orders'?'OVERVIEW':'RECOVERY'}</div><h1>{page==='orders'?'Order activity':'Dead letters'}</h1><p>{page==='orders'?'Follow orders as they move through the pipeline.':'Inspect failed messages and retry them after the issue is fixed.'}</p></div><button className="primary" disabled={busy} onClick={create}><span>+</span> Create demo order</button></div>
      {error && <div className="notice" role="alert"><CircleAlert size={17}/>{error}<button onClick={()=>setError('')} aria-label="Dismiss"><X size={16}/></button></div>}
      {page==='orders' ? <><section className="stats"><Stat label="Orders" value={orders.length} icon={<Boxes size={18}/>} caption="Latest 100 orders"/><Stat label="Confirmed" value={orders.filter(o=>o.status==='CONFIRMED').length} icon={<ShieldCheck size={18}/>} caption="Successfully completed" tone="green"/><Stat label="In progress" value={orders.filter(o=>o.status==='CREATED').length} icon={<Clock3 size={18}/>} caption="Awaiting an outcome" tone="blue"/><Stat label="Cancelled" value={orders.filter(o=>o.status==='CANCELLED').length} icon={<CircleAlert size={18}/>} caption="Require attention" tone="orange"/></section>
      <div className="layout"><section className="card orders-card"><div className="card-heading"><div><h2>Recent orders</h2><p>Latest activity across the pipeline</p></div><span className="count">{filtered.length} orders</span></div><div className="tools"><label className="search"><Search size={16}/><input value={query} onChange={e=>setQuery(e.target.value)} placeholder="Search by ID, SKU or status"/></label><span>Updated {updated ? updated.toLocaleTimeString() : '—'}</span></div><div className="table-wrap"><table><thead><tr><th>ORDER ID</th><th>ITEM</th><th>AMOUNT</th><th>STATUS</th><th>CREATED</th><th></th></tr></thead><tbody>{filtered.map(o=><tr key={o.id} onClick={()=>setSelected(o.id)} onKeyDown={e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();setSelected(o.id)}}} tabIndex={0} aria-label={`View timeline for order ${o.id}`} className={selected===o.id?'selected':''}><td className="order-id">{o.id.slice(0,8)}<small>{o.id.slice(9,18)}</small></td><td><strong>{o.sku}</strong><small>Qty {o.quantity}</small></td><td>${Number(o.totalAmount).toFixed(2)}</td><td><Status status={o.status}/></td><td className="muted">{formatDate(o.createdAt)}</td><td><ArrowUpRight size={16}/></td></tr>)}</tbody></table>{!filtered.length && <div className="empty">{loading?'Loading orders…':'No orders found. Create a demo order to get started.'}</div>}</div></section>
      <div className="right-column"><section className="card detail"><div className="card-heading"><div><h2>Event timeline</h2><p>{current ? `Order ${current.id.slice(0,8)}` : 'Select an order to inspect'}</p></div><Activity size={17} className="muted"/></div>{current ? <><div className="detail-id"><span>CORRELATION ID</span><code>{current.correlationId}</code></div><div className="timeline">{events.map((event,i)=><div className="event" key={event.eventId}><div className={'event-marker '+(event.eventType.includes('Failed')||event.eventType.includes('Cancelled')?'warn':'')}>{i===events.length-1?<span/>:null}</div><div><strong>{event.eventType.replace(/([a-z])([A-Z])/g,'$1 $2')}</strong><small>{formatDate(event.occurredAt)}</small></div></div>)}{!events.length && <div className="empty compact">Waiting for events…</div>}</div></> : <div className="empty detail-empty"><Activity size={24}/><strong>No order selected</strong><span>Choose an order to see its journey.</span></div>}</section>
      <section className="card control"><div className="control-head"><div className="control-icon"><SlidersHorizontal size={18}/></div><span className="pill">DEMO CONTROL</span></div><h2>Payment failure</h2><p>Force mock payments to fail and watch the compensation flow.</p><div className="toggle-row"><div><strong>Failure injection</strong><small>{rate === null ? 'Unavailable' : rate === 1 ? 'All payments will fail' : rate === 0 ? 'Payments are succeeding' : `${Math.round(rate*100)}% failure rate`}</small></div><button className={'toggle '+(rate===1?'on':'')} aria-label="Force payment failures" aria-pressed={rate===1} disabled={busy||rate===null} onClick={()=>mutate(()=>api('/api/payment/payment-control',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({failureRate:rate===1?0:1})}))}><span/></button></div></section></div></div></> : <section className="card"><div className="card-heading"><div><h2>Messages needing review</h2><p>Replay queues the original event for its failed service.</p></div><span className="count">{pending} pending</span></div><div className="table-wrap"><table><thead><tr><th>SERVICE</th><th>MESSAGE KEY</th><th>ERROR</th><th>RECEIVED</th><th>STATUS</th><th></th></tr></thead><tbody>{dead.map(d=><tr key={`${d.service}-${d.id}`}><td><strong>{d.service}</strong><small>#{d.id}</small></td><td className="mono">{d.messageKey || '—'}</td><td className="error-cell" title={d.errorMessage||''}>{d.errorMessage || 'No error detail'}</td><td>{formatDate(d.receivedAt)}</td><td><Status status={d.replayRequestedAt?'QUEUED':'PENDING'}/></td><td><button className="replay" disabled={busy||!!d.replayRequestedAt} onClick={()=>mutate(()=>api(`/api/${d.service}/dead-letters/${d.id}/replay`,{method:'POST'}))}>{d.replayRequestedAt?'Queued':'Replay'}</button></td></tr>)}</tbody></table>{!dead.length&&<div className="empty">No dead letters found.</div>}</div><div className="footnote">Showing up to 1,000 records per service. A queued replay can fail again and create a new entry.</div></section>}
      <footer><span><i/> AUTO-REFRESH EVERY 3 SECONDS</span><span>Order pipeline <b>·</b> Local environment</span></footer></div></main></div>
}
function Stat({label,value,icon,caption,tone=''}:{label:string;value:number;icon:React.ReactNode;caption:string;tone?:string}) {return <div className="stat"><div className="stat-top"><span>{label}</span><div className={'stat-icon '+tone}>{icon}</div></div><strong>{value.toString().padStart(2,'0')}</strong><small>{caption}</small></div>}
function Status({status}:{status:string}) {const names:Record<string,string>={CREATED:'In progress',CONFIRMED:'Confirmed',CANCELLED:'Cancelled',PENDING:'Pending',QUEUED:'Queued'};return <span className={'status '+status.toLowerCase()}><i/>{names[status] || status}</span>}
createRoot(document.getElementById('root')!).render(<React.StrictMode><App/></React.StrictMode>)
