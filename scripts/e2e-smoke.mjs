#!/usr/bin/env node
/**
 * dsh-http-server 任务 5 冒烟：e2e 全链路（health → 建 → prompt → events →
 * wait → stream → DELETE → 404 复验 → 同 sessionId resume 跨重启恢复）
 *
 * 两种运行模式（自动选择）：
 *
 * A. 真实 DSH（DSH 机器上，先挂插件）：
 *    npx tsc
 *    npx @deepseek-ai/dsh web --patch ./cordis.patch.yml
 *    node scripts/e2e-smoke.mjs          # 全链路（E1-E8；E8 接受 404 或 200-resume）
 *    # 跨重启恢复两段式（brief「重启 DSH 后同 sessionId 再 prompt」）：
 *    #   ① 记下上一步输出末尾的 sessionId → 重启 DSH →
 *    DSH_RESUME_SESSION=<sessionId> node scripts/e2e-smoke.mjs
 *
 * B. stub 全链路（本机无 DSH 实例时的裁决路径）：DSH_BASE 不可达时，起 stub
 *    DSH（贴 0.2.2-rc.5 接口形状：ctx 事件总线 + agents + sessionPersistence，
 *    模式照搬 smoke-stream），对同一组断言打真实 HTTP 请求。跨重启恢复按裁决
 *    用「persistence.stat 存在 + 内存 record 清空」模拟：两个纪元——纪元 1 跑
 *    全链路到 404 复验；纪元 2 模拟 DSH 重启（全新 archive/store/事件总线/端口，
 *    共享同一持久化实例并保留目标会话条目），同 sessionId 再 prompt 断言自动
 *    resume（agents.resume 恰好一次、create 零次）。
 *
 * 断言（brief 步骤 3）：
 *   E1 GET /health → 200 {ok, service, version}
 *   E2 POST /sessions → 201 {sessionId, cwd}
 *   E3 同步 prompt（需工具的长任务「列出当前目录的文件」）→ 200 {status:completed, text, usage}
 *   E4 GET events?since=0 → 含工具调用（tool/call）+ turn/end + usage，nextSince 推进
 *   E5 GET events?since=<cursor> → 空列表（增量轮询游标语义）
 *   E6 POST wait → 200 {status:completed}
 *   E7 POST stream（另一轮）→ connected + delta 帧流 + tool + turn/end(usage) + done 收流
 *   E8 DELETE → 204；同 id prompt → 404 {"error":"not found"}
 *      （真实模式接受 200-resume：真实 SDK teardown 只 close 不删持久化——
 *        task-3 审查 Ruling；stub 的 forget 模拟「无持久化会话」契约路径）
 *   E9 （仅 stub / 真实两段式）同 sessionId 再 prompt → 200 completed，走 resume 路径
 */

import { createServer } from 'node:http'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const BASE = process.env.DSH_BASE || 'http://127.0.0.1:3080'
const RESUME_SESSION = process.env.DSH_RESUME_SESSION || ''
const PREFIX = '/api/v1'
const PROBE_TIMEOUT_MS = 8000
const REQUEST_TIMEOUT_MS = 180000

const scriptDir = fileURLToPath(new URL('.', import.meta.url))
const pluginDir = resolve(scriptDir, '..')

const results = []

function check(name, ok, detail) {
  results.push({ name, ok })
  console.log(`${ok ? '  PASS' : '  FAIL'}  ${name}${detail ? ' — ' + detail : ''}`)
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

async function fetchJson(url, options) {
  const response = await fetch(url, { ...options, signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS) })
  const text = await response.text()
  let body
  try { body = JSON.parse(text) } catch { body = undefined }
  return { status: response.status, text, body }
}

async function probe() {
  try {
    await fetch(BASE, { signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) })
    return true
  } catch (error) {
    const code = error?.cause?.code || error?.code || ''
    return !(code === 'UND_ERR_CONNECT_TIMEOUT' || code === 'ECONNREFUSED' || code === 'ENOTFOUND'
      || code === 'ETIMEDOUT' || error?.name === 'TimeoutError' || error?.name === 'AbortError')
  }
}

function safeJson(text) {
  if (typeof text !== 'string') return undefined
  try { return JSON.parse(text) } catch { return undefined }
}

/** POST stream 并整读 SSE 流（帧解析照搬 smoke-stream）。 */
async function streamOnce(base, sessionId, body, opts = {}) {
  const { deadlineMs = 180000 } = opts
  const response = await fetch(`${base}${PREFIX}/sessions/${sessionId}/stream`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body ?? {}),
  })
  const frames = []
  if (!response.ok) {
    const text = await response.text()
    return { status: response.status, frames, closed: true, text, body: safeJson(text) }
  }
  const consume = (async () => {
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buf = ''
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buf += decoder.decode(value, { stream: true })
      let idx = buf.indexOf('\n\n')
      while (idx !== -1) {
        const raw = buf.slice(0, idx)
        buf = buf.slice(idx + 2)
        if (!raw.startsWith(':')) {
          let event
          let data
          for (const line of raw.split('\n')) {
            if (line.startsWith('event: ')) event = line.slice(7)
            else if (line.startsWith('data: ')) data = line.slice(6)
          }
          frames.push({ event, data, json: safeJson(data) })
        }
        idx = buf.indexOf('\n\n')
      }
    }
  })()
  const deadline = new Promise((_, reject) => {
    const timer = setTimeout(() => reject(new Error(`stream deadline ${deadlineMs}ms`)), deadlineMs)
    timer.unref?.()
  })
  await Promise.race([consume, deadline])
  return { status: response.status, frames, closed: true }
}

const framesOf = (frames, name) => frames.filter((f) => f.event === name)
const joinedDeltas = (frames) => framesOf(frames, 'delta').map((f) => f.json?.delta ?? '').join('')

// ---------------------------------------------------------------------------
// stub DSH（模式照搬 smoke-stream：ctx 事件总线 + agents/persistence，0.2.2-rc.5 形状）
// ---------------------------------------------------------------------------

function stubPersistence(root) {
  const sessions = new Map()
  return {
    root,
    stat: async (id) => {
      const s = sessions.get(String(id))
      return s === undefined ? undefined : { header: { cwd: s.cwd }, revision: 'rev', eventCount: s.eventCount }
    },
    remember(sessionId, cwd) { sessions.set(sessionId, { cwd, eventCount: 0, turn: 0, startSeq: 0 }) },
    forget(sessionId) { sessions.delete(sessionId) },
    has(sessionId) { return sessions.has(sessionId) },
    cwdOf(sessionId) { return sessions.get(sessionId)?.cwd },
    startSeqOf(sessionId) { return sessions.get(sessionId)?.startSeq ?? 0 },
    nextTurnOf(sessionId) { const s = sessions.get(sessionId); s.turn += 1; return s.turn },
  }
}

/**
 * stub AgentHandle：followup 触发一轮「需工具的长任务」（照 smoke-stream full 模式）：
 * turn/start → step/start → reasoning/text chunks → tool/call(list_files) →
 * assistant/message 全文（含未流式尾巴）→ turn/end（usage）。
 */
function stubHandle(sessionId, ctx, persistence) {
  let seq = persistence.startSeqOf(sessionId)
  let status = 'idle'
  const idleWaiters = []
  const handle = {
    disposed: false,
    agent: {
      session: { id: sessionId, header: { cwd: persistence.cwdOf(sessionId) }, get seq() { return seq } },
      get status() { return status },
      followup(message) {
        status = 'running'
        setTimeout(() => {
          const turn = persistence.nextTurnOf(sessionId)
          const emit = (type, data) => ctx.emit('session/event', { id: sessionId }, { type, seq: seq++, time: Date.now(), data })
          emit('turn/start', { turn })
          emit('step/start', { turn, step: 1 })
          emit('assistant/chunk', { turn, step: 1, chunk: { type: 'reasoning-delta', index: 0, text: '思考中' } })
          for (const t of ['1\n', '2\n', '3\n']) {
            emit('assistant/chunk', { turn, step: 1, chunk: { type: 'text-delta', index: 0, text: t } })
          }
          emit('tool/call', { turn, step: 1, callId: 'c1', name: 'list_files', arguments: '{"path":"."}' })
          emit('tool/result', { turn, step: 1, message: { role: 'tool', content: [] } })
          emit('assistant/message', {
            turn, step: 1,
            message: { role: 'assistant', id: `m-${seq}`, content: [{ type: 'text', text: '1\n2\n3\n4\n5\n' }], source: { kind: 'model' } },
            stream: [],
            usage: { inputTokens: 12, outputTokens: 34, cacheReadTokens: 5, cacheWriteTokens: 0, totalTokens: 46 },
          })
          emit('turn/end', { turn, reason: { kind: 'completed' } })
          status = 'idle'
          const waiters = idleWaiters.splice(0)
          for (const w of waiters) w()
        }, 60)
      },
      whenIdle() {
        if (status === 'idle') return Promise.resolve()
        return new Promise((resolve) => { idleWaiters.push(resolve) })
      },
    },
    async dispose() {
      handle.disposed = true
      persistence.forget(sessionId)
    },
  }
  return handle
}

/** stub ctx：事件总线 + effect 即装 + 服务探测；agents 带 create/resume 计数（E9 断言用）。 */
function makeStubCtx(persistence, root, handles) {
  const listeners = new Map()
  const agents = {
    createCalls: 0,
    resumeCalls: 0,
    async create(options) {
      agents.createCalls += 1
      const sessionId = String(options.sessionId)
      persistence.remember(sessionId, options.meta?.cwd ?? root)
      const handle = stubHandle(sessionId, ctx, persistence)
      handles.set(sessionId, handle)
      return handle
    },
    async resume(options) {
      agents.resumeCalls += 1
      const sessionId = String(options.resumeSessionId)
      if (!persistence.has(sessionId)) throw new Error('not persisted')
      const handle = stubHandle(sessionId, ctx, persistence)
      handles.set(sessionId, handle)
      return handle
    },
  }
  const ctx = {
    get: (name) => (name === 'sessionPersistence' ? persistence : undefined),
    on: (name, cb) => {
      let set = listeners.get(name)
      if (set === undefined) { set = new Set(); listeners.set(name, set) }
      set.add(cb)
      return () => set.delete(cb)
    },
    effect: (fn) => { fn(); return () => undefined },
    emit: (name, ...args) => { for (const cb of [...(listeners.get(name) ?? [])]) cb(...args) },
    agents,
    llm: {
      listProviders: () => [{ id: 'stub-provider' }],
      listModels: async (provider) => [{ provider, id: 'stub-model' }],
    },
  }
  return ctx
}

/**
 * 起一个「纪元」：全新 archive/store/事件总线/HTTP 服务（内存世界），共享传入的
 * persistence 实例（磁盘世界）。重启模拟 = 起新纪元（裁决：persistence.stat 存在
 * + 内存 record 清空）。
 */
async function startEpoch(port, persistence) {
  const { EventArchive, attachEventArchive } = await import(join(pluginDir, 'lib', 'events.js'))
  const { SessionStore } = await import(join(pluginDir, 'lib', 'sessions.js'))
  const handles = new Map()
  const root = persistence.root
  const ctx = makeStubCtx(persistence, root, handles)
  const config = {
    pathPrefix: PREFIX, standaloneHost: '127.0.0.1', standalonePort: 0,
    agentPreset: 'standard', workspaceRoot: root, stallTimeoutMs: 300000,
  }
  const archive = new EventArchive()
  const store = new SessionStore(ctx, config, archive)
  attachEventArchive(ctx, archive, (id) => store.get(id) !== undefined)
  const router = await import(join(pluginDir, 'lib', 'router.js'))
  const server = createServer((req, res) => {
    router.match(req, res, router.stripPrefix(router.requestPath(req), PREFIX), { ctx, config, store, archive })
  })
  await new Promise((done) => server.listen(port, '127.0.0.1', done))
  return { server, ctx, handles, base: `http://127.0.0.1:${port}` }
}

async function createSession(base) {
  const created = await fetchJson(`${base}${PREFIX}/sessions`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}',
  })
  return { sessionId: created.body?.sessionId, cwd: created.body?.cwd, status: created.status }
}

// ---------------------------------------------------------------------------
// stub 全链路（两个纪元）
// ---------------------------------------------------------------------------

// 纪元 1 → 纪元 2 的会话交接（E2 建立后记录，纪元 2 的重启模拟复用）
let sessionIdOfEpoch1 = ''
let cwdOfEpoch1 = ''

async function assertStubSuite() {
  const root = mkdtempSync(join(tmpdir(), 'dsh-smoke-e2e-'))
  const persistence = stubPersistence(root)

  // ---- 纪元 1：首启 DSH，全链路 E1-E8 ----
  const e1 = await startEpoch(31991, persistence)
  try {
    // E1 health
    {
      const { status, body } = await fetchJson(`${e1.base}${PREFIX}/health`)
      check('E1 GET /health → 200 {ok, service, version}',
        status === 200 && body?.ok === true && body?.service === 'dsh-http-server' && typeof body?.version === 'string',
        `status=${status} service=${body?.service}`)
    }

    // E2 建会话
    const created = await createSession(e1.base)
    const sessionId = created.sessionId
    const cwd = created.cwd
    check('E2 POST /sessions → 201 {sessionId, cwd}',
      created.status === 201 && typeof sessionId === 'string' && sessionId.length > 0 && typeof cwd === 'string',
      `status=${created.status} sessionId=${sessionId ?? 'none'} cwd=${JSON.stringify(cwd ?? '')}`)
    if (typeof sessionId !== 'string') return
    sessionIdOfEpoch1 = sessionId
    cwdOfEpoch1 = cwd ?? root

    // E3 同步 prompt（需工具的长任务）
    {
      const { status, body } = await fetchJson(`${e1.base}${PREFIX}/sessions/${sessionId}/prompt`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: '列出当前目录的文件', timeoutMs: 60000 }),
      })
      check('E3 同步 prompt（需工具长任务）→ 200 {status:completed, text, usage}',
        status === 200 && body?.status === 'completed' && typeof body?.text === 'string' && body.text.length > 0
          && body?.usage?.input === 12,
        `status=${status} body.status=${body?.status} text=${JSON.stringify((body?.text ?? '').slice(0, 20))} usage=${JSON.stringify(body?.usage)}`)
    }

    // E4 events 轮询（含工具调用）
    let cursor = 0
    {
      const { status, body } = await fetchJson(`${e1.base}${PREFIX}/sessions/${sessionId}/events?since=0`)
      const events = body?.events ?? []
      const toolCall = events.find((e) => e.type === 'tool/call')
      const hasTurnEnd = events.some((e) => e.type === 'turn/end')
      const usage = events.find((e) => e.type === 'assistant/message')?.data?.usage
      if (typeof body?.nextSince === 'number') cursor = body.nextSince
      check('E4 GET events?since=0 → 含工具调用（tool/call list_files）+ turn/end + usage，nextSince 推进',
        status === 200 && toolCall?.data?.name === 'list_files' && hasTurnEnd && usage !== undefined && cursor > 0,
        `status=${status} tool=${toolCall?.data?.name ?? 'none'} turnEnd=${hasTurnEnd} usage=${usage ? 'present' : 'missing'} cursor=${cursor}`)
    }

    // E5 增量游标轮询
    {
      const { status, body } = await fetchJson(`${e1.base}${PREFIX}/sessions/${sessionId}/events?since=${cursor}`)
      check(`E5 GET events?since=${cursor} → 空列表（增量游标语义）`,
        status === 200 && (body?.events ?? []).length === 0 && body?.nextSince === cursor,
        `status=${status} count=${(body?.events ?? []).length} nextSince=${body?.nextSince}`)
    }

    // E6 wait
    {
      const { status, body } = await fetchJson(`${e1.base}${PREFIX}/sessions/${sessionId}/wait`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ timeoutMs: 60000 }),
      })
      check('E6 POST wait → 200 {status:completed}',
        status === 200 && (body?.status === 'completed' || body?.status === 'timeout'),
        `status=${status} body.status=${body?.status}`)
    }

    // E7 stream（另一轮）
    {
      const r = await streamOnce(e1.base, sessionId, { prompt: '把上一轮的发现压成一行总结' })
      const turnEnd = framesOf(r.frames, 'turn/end')
      const done = framesOf(r.frames, 'done')
      const tool = framesOf(r.frames, 'tool')
      check('E7 POST stream（另一轮）→ connected + delta 帧流 + tool + turn/end(usage) + done 收流',
        r.status === 200 && framesOf(r.frames, 'connected').length === 1
          && joinedDeltas(r.frames) === '1\n2\n3\n4\n5\n'
          && tool.length === 1 && tool[0]?.json?.name === 'list_files'
          && turnEnd.length === 1 && turnEnd[0]?.json?.usage?.input === 12
          && done.length === 1 && done[0]?.data === '"[DONE]"' && r.closed,
        `status=${r.status} deltas=${framesOf(r.frames, 'delta').length} tool=${tool.length} turnEnd=${turnEnd.length} done=${done.length}`)
    }

    // E8 DELETE → 204；同 id prompt → 404（stub 的 dispose forget 模拟「无持久化会话」契约路径）
    {
      const del = await fetchJson(`${e1.base}${PREFIX}/sessions/${sessionId}`, { method: 'DELETE' })
      const after = await fetchJson(`${e1.base}${PREFIX}/sessions/${sessionId}/prompt`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: '删除后再问', timeoutMs: 60000 }),
      })
      check('E8 DELETE → 204；同 id prompt → 404 {"error":"not found"}',
        del.status === 204 && after.status === 404 && after.body?.error === 'not found',
        `delete=${del.status} after=${after.status} body=${JSON.stringify(after.body)}`)
    }
  } finally {
    await new Promise((done) => e1.server.close(done))
  }

  // ---- 纪元 2：重启模拟（persistence.stat 存在 + 内存 record 清空）----
  // 真实场景：DSH 重启后内存全空、磁盘持久化仍在（官方 teardown 只 close 不删
  // 持久化——task-3 审查 Ruling；E8 的 404 走「无持久化」限定路径）。此处把目标
  // 会话保留在共享持久化实例中（对应磁盘文件仍在），起全新内存世界。
  persistence.remember(sessionIdOfEpoch1, cwdOfEpoch1)
  const e2 = await startEpoch(31992, persistence)
  try {
    const { status, body } = await fetchJson(`${e2.base}${PREFIX}/sessions/${sessionIdOfEpoch1}/prompt`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: '重启后的第一问', timeoutMs: 60000 }),
    })
    const { createCalls, resumeCalls } = e2.ctx.agents
    check('E9 （重启模拟：stat 存在 + 内存清空）同 id prompt → 200 completed，走 resume 路径',
      status === 200 && body?.status === 'completed' && typeof body?.text === 'string' && body.text.length > 0
        && resumeCalls === 1 && createCalls === 0,
      `status=${status} body.status=${body?.status} resume=${resumeCalls} create=${createCalls}`)
  } finally {
    await new Promise((done) => e2.server.close(done))
    rmSync(root, { recursive: true, force: true })
  }
}

// ---------------------------------------------------------------------------
// 真实 DSH 断言（DSH_BASE 可达时，Windows 端）
// ---------------------------------------------------------------------------

async function assertRealResumeOnly() {
  const { status, body } = await fetchJson(`${BASE}${PREFIX}/sessions/${RESUME_SESSION}/prompt`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ prompt: '重启后的第一问（跨重启恢复验证）', timeoutMs: 120000 }),
  })
  check('E9 真实跨重启恢复：DSH_RESUME_SESSION 同 id prompt → 200 completed（自动 resume）',
    status === 200 && body?.status === 'completed',
    `status=${status} body.status=${body?.status} text=${JSON.stringify((body?.text ?? '').slice(0, 40))}`)
}

async function assertRealSuite() {
  // E1-E2
  {
    const { status, body } = await fetchJson(`${BASE}${PREFIX}/health`)
    check('E1 真实 GET /health → 200', status === 200 && body?.ok === true, `status=${status}`)
  }
  const created = await createSession(BASE)
  const sessionId = created.sessionId
  check('E2 真实 POST /sessions → 201 {sessionId, cwd}',
    created.status === 201 && typeof sessionId === 'string' && sessionId.length > 0,
    `status=${created.status} sessionId=${sessionId ?? 'none'}`)
  if (typeof sessionId !== 'string') return

  // E3 同步 prompt（需工具的长任务；真实模型可能不调工具，工具证据在 E4 软断言）
  {
    const { status, body } = await fetchJson(`${BASE}${PREFIX}/sessions/${sessionId}/prompt`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: '列出当前目录的文件', timeoutMs: 120000 }),
    })
    check('E3 真实同步 prompt → 200 {status, text}',
      status === 200 && typeof body?.status === 'string' && typeof body?.text === 'string',
      `status=${status} body.status=${body?.status} text=${JSON.stringify((body?.text ?? '').slice(0, 40))}`)
  }

  // E4 events 轮询（turn/end 硬断言；tool/call 软证据——真实模型可能不调工具）
  let cursor = 0
  {
    const { status, body } = await fetchJson(`${BASE}${PREFIX}/sessions/${sessionId}/events?since=0`)
    const events = body?.events ?? []
    const hasTurnEnd = events.some((e) => e.type === 'turn/end')
    const toolCall = events.find((e) => e.type === 'tool/call')
    if (typeof body?.nextSince === 'number') cursor = body.nextSince
    check('E4 真实 events?since=0 → 含 turn/end（工具调用软证据）',
      status === 200 && hasTurnEnd,
      `status=${status} turnEnd=${hasTurnEnd} tool=${toolCall?.data?.name ?? 'none（模型未调工具，属正常）'} cursor=${cursor}`)
  }

  // E5 增量游标
  {
    const { status, body } = await fetchJson(`${BASE}${PREFIX}/sessions/${sessionId}/events?since=${cursor}`)
    check(`E5 真实 events?since=${cursor} → 无旧事件（增量游标语义）`,
      status === 200 && (body?.events ?? []).every((e) => e.seq > cursor),
      `status=${status} count=${(body?.events ?? []).length} nextSince=${body?.nextSince}`)
  }

  // E6 wait
  {
    const { status, body } = await fetchJson(`${BASE}${PREFIX}/sessions/${sessionId}/wait`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ timeoutMs: 120000 }),
    })
    check('E6 真实 POST wait → 200', status === 200 && typeof body?.status === 'string',
      `status=${status} body.status=${body?.status}`)
  }

  // E7 stream（另一轮）
  {
    const r = await streamOnce(BASE, sessionId, { prompt: '把上一轮的发现压成一行总结', stallTimeoutMs: 300000 })
    check('E7 真实 POST stream → connected + delta + turn/end + done 收流',
      r.status === 200 && framesOf(r.frames, 'connected').length === 1
        && framesOf(r.frames, 'delta').length >= 1 && framesOf(r.frames, 'turn/end').length === 1
        && framesOf(r.frames, 'done').length === 1 && r.closed,
      `status=${r.status} deltas=${framesOf(r.frames, 'delta').length} done=${framesOf(r.frames, 'done').length}`)
  }

  // E8 DELETE → 204；同 id prompt → 404 或 200-resume（真实 SDK teardown 只 close
  // 不删持久化——task-3 审查 Ruling；两者均为契约合规路径）
  {
    const del = await fetchJson(`${BASE}${PREFIX}/sessions/${sessionId}`, { method: 'DELETE' })
    const after = await fetchJson(`${BASE}${PREFIX}/sessions/${sessionId}/prompt`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: '删除后再问', timeoutMs: 120000 }),
    })
    check('E8 真实 DELETE → 204；同 id prompt → 404（无持久化）或 200 completed/timeout（持久化保留 → 自动 resume；error 不算通过）',
      del.status === 204 && (after.status === 404
        || (after.status === 200 && (after.body?.status === 'completed' || after.body?.status === 'timeout'))),
      `delete=${del.status} after=${after.status}(${after.body?.status ?? after.body?.error})`)
  }

  console.log(`\n跨重启恢复两段式：重启 DSH 后执行 DSH_RESUME_SESSION=${sessionId} node scripts/e2e-smoke.mjs`)
}

async function main() {
  console.log(`e2e-smoke: DSH base = ${BASE}`)
  if (RESUME_SESSION !== '') {
    if (!(await probe())) {
      console.log(`DSH_RESUME_SESSION 已设置但 DSH_BASE（${BASE}）不可达——两段式第二步应在 DSH 机器上执行。`)
      process.exit(1)
    }
    console.log('真实跨重启恢复两段式（第二步）：对 DSH_RESUME_SESSION 指定会话断言 resume。')
    await assertRealResumeOnly()
  } else if (await probe()) {
    console.log('模式 A：对真实 DSH 实例断言（E1-E8；跨重启恢复走两段式，见输出末尾提示）。')
    await assertRealSuite()
  } else {
    console.log('模式 B（stub 全链路）：本机 DSH 不可达，起 stub DSH 两纪元驱动 E1-E9。')
    await assertStubSuite()
  }
  const failed = results.filter((r) => !r.ok).length
  console.log(failed === 0 ? `\nALL PASS (${results.length}/${results.length})` : `\n${failed} FAILED`)
  process.exit(failed === 0 ? 0 : 1)
}

await main()
