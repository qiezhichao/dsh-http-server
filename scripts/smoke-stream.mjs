#!/usr/bin/env node
/**
 * dsh-http-server 任务 4 冒烟：SSE 流式端点 POST /sessions/:id/stream
 *
 * 运行结构（裁决钉死）：
 *   ① sse.ts 基建直检（initSseStream 头/帧格式 + startSseHeartbeat 注释帧/停止函数）
 *   ② stub 冒烟（恒跑，贴 0.2.2-rc.5 接口形状，模拟 delta 帧流/turn/end/心跳/409）：
 *      起真实 node:http 服务，加载编译产物 lib/*.js + stub ctx（事件总线喂归档与
 *      SSE 双订阅），对断言打真实 HTTP 请求。
 *   ③ 真实 DSH：DSH_BASE 可达才补跑（Windows 端用）；不可达 → SKIP 退出码 0。
 *
 * 断言（brief 步骤 2 + 裁决门闩语义）：
 *   S1 connected / delta 帧流（chunks+差量补推）/ reasoning / tool / turn/end(usage) / done 后收流
 *   S2 harness 不发 chunk 只给 assistant/message 全文 → 差量补推 delta
 *   S3 停滞看门狗（stallTimeoutMs=1000 且 stub 静默）→ error+done 收流；且收流后门闩已清（再 stream → 200 非 409）
 *   S4 忙时第二个 stream → 409 {"error":"session busy"}
 *   S5 客户端断开（abort）→ 门闩已清（再 prompt 不 409）
 *   S6 心跳基建（short-interval 直检：每 interval 写 `: ping`、停止后不再写）
 *   S7 body 无 prompt → 400；未知 session → 404
 *   S8 turn 归属：断开重连后旧轮 turn/end 不收流（done 唯一且在第二轮之后）
 *   S9 会话级 error 事件 → error 帧（真实错误信息）+ done 即时收流（不等看门狗）
 *   真实 DSH（可达时）：R1 正常流 / R2 409 / R3 400 / R4 404 / R5 abort 后门闩已清
 *
 * 注：客户端断开探测依赖 ServerResponse 'close'（Node ≥16 的 IncomingMessage
 * 'close' 在 body 消费完即发且断开时不再触发，实测已证；详见 router.ts 注释）。
 */

import { createServer } from 'node:http'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const BASE = process.env.DSH_BASE || 'http://127.0.0.1:3080'
const PREFIX = '/api/v1'
const PROBE_TIMEOUT_MS = 8000

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
  const response = await fetch(url, { ...options, signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) })
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

/** POST stream 并整读 SSE 流：帧解析（event:/data:/注释行），支持 abort 信号与截止时间。 */
async function streamOnce(base, sessionId, body, opts = {}) {
  const { signal, onFrame, deadlineMs = 30000 } = opts
  const response = await fetch(`${base}${PREFIX}/sessions/${sessionId}/stream`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body ?? {}),
    signal,
  })
  const frames = []
  let aborted = false
  let closed = false
  if (!response.ok) {
    const text = await response.text()
    return { status: response.status, frames, aborted: false, closed: true, text, body: safeJson(text) }
  }
  const push = (frame) => { frames.push(frame); if (onFrame !== undefined) onFrame(frame) }
  const consume = (async () => {
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buf = ''
    try {
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buf += decoder.decode(value, { stream: true })
        let idx = buf.indexOf('\n\n')
        while (idx !== -1) {
          const raw = buf.slice(0, idx)
          buf = buf.slice(idx + 2)
          if (raw.startsWith(':')) {
            push({ comment: raw })
          } else {
            let event
            let data
            for (const line of raw.split('\n')) {
              if (line.startsWith('event: ')) event = line.slice(7)
              else if (line.startsWith('data: ')) data = line.slice(6)
            }
            push({ event, data, json: safeJson(data) })
          }
          idx = buf.indexOf('\n\n')
        }
      }
      closed = true
    } catch (error) {
      if (signal?.aborted) { aborted = true; return }
      throw error
    }
  })()
  const deadline = new Promise((_, reject) => {
    const timer = setTimeout(() => reject(new Error(`stream deadline ${deadlineMs}ms`)), deadlineMs)
    timer.unref?.()
  })
  await Promise.race([consume, deadline])
  return { status: response.status, frames, aborted, closed }
}

const framesOf = (frames, name) => frames.filter((f) => f.event === name)
const joinedDeltas = (frames) => framesOf(frames, 'delta').map((f) => f.json?.delta ?? '').join('')

// ---------------------------------------------------------------------------
// S6：sse.ts 基建直检（不依赖服务器；心跳用短 interval 直验注释帧机制，路由侧仍 15s）
// ---------------------------------------------------------------------------

async function assertSseInfra() {
  const { initSseStream, startSseHeartbeat } = await import(join(pluginDir, 'lib', 'sse.js'))
  const writes = []
  const fakeRes = {
    writeHead(code, headers) { fakeRes.code = code; fakeRes.headers = headers },
    write(text) { writes.push(String(text)) },
    end() { fakeRes.ended = true },
    get writableEnded() { return fakeRes.ended === true },
  }
  const sse = initSseStream(fakeRes)
  check('S6a initSseStream 写 200 + text/event-stream 头',
    fakeRes.code === 200 && fakeRes.headers?.['Content-Type'] === 'text/event-stream',
    `code=${fakeRes.code} headers=${JSON.stringify(fakeRes.headers)}`)
  sse.send('delta', { delta: 'x' })
  sse.send('done', '[DONE]')
  check('S6b send 帧格式 event:/data: 全量 JSON（done 即 data: "[DONE]"）',
    writes.includes('event: delta\ndata: {"delta":"x"}\n\n') && writes.includes('event: done\ndata: "[DONE]"\n\n'),
    JSON.stringify(writes))
  const stop = startSseHeartbeat(sse, 100)
  await sleep(280)
  const pings1 = writes.filter((w) => w === ': ping\n\n').length
  stop()
  await sleep(280)
  const pings2 = writes.filter((w) => w === ': ping\n\n').length
  check('S6c 心跳每 interval 写 `: ping` 注释帧且停止函数生效',
    pings1 >= 1 && pings2 === pings1, `pings@280ms=${pings1} pings@+280ms=${pings2}`)
  sse.close()
  const before = writes.length
  sse.send('delta', { delta: 'y' })
  check('S6d close 后 send/ping 幂等', fakeRes.ended === true && writes.length === before,
    `ended=${fakeRes.ended} writes=${writes.length}`)
}

// ---------------------------------------------------------------------------
// stub DSH：贴 0.2.2-rc.5 接口形状的最小 ctx（事件总线）/ agents / sessionPersistence。
// 事件经 ctx.emit('session/event', {id}, event) 分发——归档（attachEventArchive
// 生产代码）与 SSE 路由自持订阅走同一条总线，与真实装配一致。
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
 * stub AgentHandle（0.2.2-rc.5 形状：agent.session.id/status/followup/whenIdle）。
 * handle.mode 控制 followup 行为（测试在 handles.get(id).mode 上改）：
 *  - 'full'：60ms 速完一轮：turn/start → step/start → reasoning/text chunks → tool/call
 *    → assistant/message（全文=已流式片段 + 未流式尾巴「4\n5\n」，带 usage）→ turn/end
 *  - 'no-chunks'：不发任何 chunk，只 assistant/message 全文（差量补推场景）
 *  - 'silent'：turn 永不开始不发事件（停滞看门狗场景）
 *  - 'slow'：turn/start 立即，chunks 每 200ms 一片、1200ms 收尾（409/断开/turn 归属场景）
 *  - 'error'：turn/start 后注入会话级 error 事件（带真实 message），不发 step/turn/end
 *    ——严格模拟 error 终结流且无正常收尾：缺 error 分支时流只能等看门狗（S9 场景）
 */
function stubHandle(sessionId, ctx, persistence) {
  let seq = persistence.startSeqOf(sessionId)
  let status = 'idle'
  const idleWaiters = []
  const queue = []
  const handle = {
    mode: 'full',
    disposed: false,
    agent: {
      session: { id: sessionId, header: { cwd: persistence.cwdOf(sessionId) }, get seq() { return seq } },
      get status() { return status },
      followup(message) {
        if (status === 'running') { queue.push(message); return }
        runTurn(message)
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
  const emit = (type, data) => {
    if (handle.disposed) return
    ctx.emit('session/event', { id: sessionId }, { type, seq: seq++, time: Date.now(), data })
  }
  // token 级增量走 agent/assistant-stream（对齐真实 DSH：chunk 帧是 agent 作用域
  // cordis 事件，不发进 session/event 持久化总线——插件的双订阅各自消费）。
  const emitStream = (type, chunk) => {
    if (handle.disposed) return
    ctx.emit('agent/assistant-stream', { agent: { session: { id: sessionId } }, frame: { type, attemptId: 'a1', revision: 1, index: 0, time: Date.now(), chunk } })
  }
  const finishTurn = (turn, fullText, usage) => {
    emit('assistant/message', {
      turn, step: 1,
      message: { role: 'assistant', id: `m-${seq}`, content: [{ type: 'text', text: fullText }], source: { kind: 'model' } },
      stream: [], usage,
    })
    emit('turn/end', { turn, reason: { kind: 'completed' } })
    status = 'idle'
    const waiters = idleWaiters.splice(0)
    for (const w of waiters) w()
    const next = queue.shift()
    if (next !== undefined) runTurn(next)
  }
  const runTurn = (message) => {
    void message
    const turn = persistence.nextTurnOf(sessionId)
    status = 'running'
    if (handle.mode === 'silent') return
    if (handle.mode === 'slow') {
      emit('turn/start', { turn })
      emit('step/start', { turn, step: 1 })
      for (let k = 0; k < 4; k++) {
        setTimeout(() => emitStream('chunk', { type: 'text-delta', text: `第${turn}轮回复片段${k} ` }), 200 * (k + 1))
      }
      setTimeout(() => finishTurn(turn, `第${turn}轮回复全文`,
        { inputTokens: 11, outputTokens: 22, cacheReadTokens: 2, cacheWriteTokens: 1, totalTokens: 33 }), 1200)
      return
    }
    setTimeout(() => {
      emit('turn/start', { turn })
      if (handle.mode === 'error') {
        // 会话级 error 事件终结流：不发 step/turn/end（严格模拟无正常收尾），
        // 真实错误信息随 data.message 透传；agent 回 idle（错误后不再跑）。
        emit('error', { message: `stub: turn ${turn} 模型调用失败（注入错误）` })
        status = 'idle'
        const waiters = idleWaiters.splice(0)
        for (const w of waiters) w()
        return
      }
      emit('step/start', { turn, step: 1 })
      if (handle.mode === 'full') {
        emitStream('chunk', { type: 'reasoning-delta', text: '思考中' })
        for (const t of ['1\n', '2\n', '3\n']) {
          emitStream('chunk', { type: 'text-delta', text: t })
        }
        emit('tool/call', { turn, step: 1, callId: 'c1', name: 'list_files', arguments: '{"path":"."}' })
        emit('tool/result', { turn, step: 1, message: { role: 'tool', content: [] } })
        // 全文含未流式尾巴「4\n5\n」→ 差量补推必须补上且不重复
        finishTurn(turn, '1\n2\n3\n4\n5\n',
          { inputTokens: 12, outputTokens: 34, cacheReadTokens: 5, cacheWriteTokens: 0, totalTokens: 46 })
      } else {
        // no-chunks：只给全文（差量补推从零起补）
        finishTurn(turn, '你好，这是一次全文推送。',
          { inputTokens: 7, outputTokens: 9, cacheReadTokens: 0, cacheWriteTokens: 0, totalTokens: 16 })
      }
    }, 60)
  }
  return handle
}

/** stub ctx：session/event 事件总线（on 返回退订器）+ effect 即装 + agents/persistence。 */
function makeStubCtx(persistence, root, handles) {
  const listeners = new Map()
  const ctx = {
    get: (name) => (name === 'sessionPersistence' ? persistence : undefined),
    on: (name, cb) => {
      let set = listeners.get(name)
      if (set === undefined) { set = new Set(); listeners.set(name, set) }
      set.add(cb)
      return () => set.delete(cb)
    },
    effect: (fn) => fn(),
    emit: (name, ...args) => { for (const cb of [...(listeners.get(name) ?? [])]) cb(...args) },
    agents: {
      async create(options) {
        const sessionId = String(options.sessionId)
        persistence.remember(sessionId, options.meta?.cwd ?? root)
        const handle = stubHandle(sessionId, ctx, persistence)
        handles.set(sessionId, handle)
        return handle
      },
      async resume(options) {
        const sessionId = String(options.resumeSessionId)
        if (!persistence.has(sessionId)) throw new Error('not persisted')
        const handle = stubHandle(sessionId, ctx, persistence)
        handles.set(sessionId, handle)
        return handle
      },
    },
  }
  return ctx
}

async function startStubDsh(port) {
  const root = mkdtempSync(join(tmpdir(), 'dsh-smoke-sse-'))
  const persistence = stubPersistence(root)
  const { EventArchive, attachEventArchive } = await import(join(pluginDir, 'lib', 'events.js'))
  const { SessionStore } = await import(join(pluginDir, 'lib', 'sessions.js'))
  const handles = new Map()
  const ctx = makeStubCtx(persistence, root, handles)
  const config = {
    pathPrefix: '/api/v1', standaloneHost: '127.0.0.1', standalonePort: 0,
    agentPreset: 'standard', workspaceRoot: root, stallTimeoutMs: 300000,
  }
  const archive = new EventArchive()
  const store = new SessionStore(ctx, config, archive)
  // 生产同款装配：归档走 ctx.on('session/event')（在册过滤），与 SSE 自持订阅同总线
  attachEventArchive(ctx, archive, (id) => store.get(id) !== undefined)
  const router = await import(join(pluginDir, 'lib', 'router.js'))
  const server = createServer((req, res) => {
    router.match(req, res, router.stripPrefix(router.requestPath(req), PREFIX), { ctx, config, store, archive })
  })
  await new Promise((done) => server.listen(port, '127.0.0.1', done))
  return {
    server, root, persistence, handles,
    cleanup: () => { server.close(); rmSync(root, { recursive: true, force: true }) },
  }
}

async function createSession(base, body = '{}') {
  const created = await fetchJson(`${base}${PREFIX}/sessions`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body })
  return created.body?.sessionId
}

// ---------------------------------------------------------------------------
// stub 套件断言主体
// ---------------------------------------------------------------------------

async function assertStubSuite(base, stub) {
  // S1 full：connected / reasoning / delta 帧流（chunks+补推）/ tool / turn/end(usage) / done 后收流
  {
    const sessionId = await createSession(base)
    const r = await streamOnce(base, sessionId, { prompt: '数到5，每个数字一行' })
    const connected = framesOf(r.frames, 'connected')
    check('S1 connected 帧（data: {sessionId}）',
      r.status === 200 && connected.length === 1 && connected[0]?.json?.sessionId === sessionId,
      `status=${r.status} connected=${connected.length}`)
    const deltas = framesOf(r.frames, 'delta')
    const seqOk = deltas.every((f) => typeof f.json?.delta === 'string' && typeof f.json?.seq === 'number')
    check('S1 delta 帧流拼接成全文（chunks + 差量补推「4\\n5\\n」，无重复）',
      deltas.length >= 3 && seqOk && joinedDeltas(r.frames) === '1\n2\n3\n4\n5\n',
      `deltas=${deltas.length} text=${JSON.stringify(joinedDeltas(r.frames))}`)
    const reasoning = framesOf(r.frames, 'reasoning')
    check('S1 reasoning 帧（reasoning-delta → {delta, seq}）',
      reasoning.length === 1 && reasoning[0]?.json?.delta === '思考中',
      `reasoning=${reasoning.length}`)
    const tool = framesOf(r.frames, 'tool')
    check('S1 tool 帧（tool/call → {name, arguments, seq}）',
      tool.length === 1 && tool[0]?.json?.name === 'list_files' && tool[0]?.json?.arguments === '{"path":"."}',
      `tool=${tool.length} json=${JSON.stringify(tool[0]?.json)}`)
    const turnEnd = framesOf(r.frames, 'turn/end')
    const done = framesOf(r.frames, 'done')
    const usage = turnEnd[0]?.json?.usage
    check('S1 turn/end 帧含 reason+usage（短名投影，与 REST 同源）后 done "[DONE]" 收流',
      turnEnd.length === 1 && turnEnd[0]?.json?.reason?.kind === 'completed'
        && usage?.input === 12 && usage?.output === 34 && usage?.cacheRead === 5 && usage?.cacheWrite === 0
        && done.length === 1 && done[0]?.data === '"[DONE]"' && r.closed,
      `turn/end=${JSON.stringify(turnEnd[0]?.json)} done=${done.length} closed=${r.closed}`)
  }

  // S2 no-chunks：harness 只给 assistant/message 全文 → 单条差量补推 delta
  {
    const sessionId = await createSession(base)
    stub.handles.get(sessionId).mode = 'no-chunks'
    const r = await streamOnce(base, sessionId, { prompt: '直接全文' })
    check('S2 不发 chunk 只给 assistant/message 全文 → 差量补推 delta',
      r.status === 200 && joinedDeltas(r.frames) === '你好，这是一次全文推送。'
        && framesOf(r.frames, 'done').length === 1 && r.closed,
      `text=${JSON.stringify(joinedDeltas(r.frames))} done=${framesOf(r.frames, 'done').length} closed=${r.closed}`)
  }

  // S3 停滞看门狗：stallTimeoutMs=1000 且 stub 静默 → error+done 收流；且门闩已清（再 stream → 200）
  {
    const sessionId = await createSession(base)
    stub.handles.get(sessionId).mode = 'silent'
    const r = await streamOnce(base, sessionId, { prompt: '停滞一轮', stallTimeoutMs: 1000 })
    const error = framesOf(r.frames, 'error')
    const done = framesOf(r.frames, 'done')
    check('S3 看门狗：1s 无会话事件 → error + done 收流',
      r.status === 200 && error.length === 1 && String(error[0]?.json?.message ?? '').includes('turn stalled')
        && done.length === 1 && r.closed,
      `error=${JSON.stringify(error[0]?.json)} done=${done.length} closed=${r.closed}`)
    const again = await streamOnce(base, sessionId, { prompt: '停滞二轮', stallTimeoutMs: 1000 })
    check('S3 看门狗收流后门闩已清（再 stream → 200 非 409）',
      again.status === 200, `status=${again.status}`)
  }

  // S4 忙时并发第二个 stream → 409 {"error":"session busy"}
  {
    const sessionId = await createSession(base)
    stub.handles.get(sessionId).mode = 'slow'
    const first = streamOnce(base, sessionId, { prompt: '慢速第一问' })
    await sleep(250)
    const second = await streamOnce(base, sessionId, { prompt: '并发第二问' })
    const firstResult = await first
    check('S4 忙时第二个 stream → 409 {"error":"session busy"}，第一个正常收流',
      second.status === 409 && second.body?.error === 'session busy' && firstResult.closed
        && framesOf(firstResult.frames, 'done').length === 1,
      `second=${second.status}(${JSON.stringify(second.body)}) first closed=${firstResult.closed}`)
  }

  // S5 客户端断开（abort）→ 门闩已清（再 prompt 不 409）
  {
    const sessionId = await createSession(base)
    stub.handles.get(sessionId).mode = 'slow'
    const controller = new AbortController()
    const r = await streamOnce(base, sessionId, { prompt: '断开场景' }, {
      signal: controller.signal,
      onFrame: (f) => { if (f.event === 'delta' && !controller.signal.aborted) controller.abort() },
    })
    const after = await (async () => {
      // abort → res 'close' → cleanup 是服务端异步路径（实测 ~10ms），
      // 稍候再发 prompt，避免抢在 clearBusy 之前形成假 409。
      await sleep(250)
      return fetchJson(`${base}${PREFIX}/sessions/${sessionId}/prompt`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prompt: '断开后再问', timeoutMs: 30000 }),
      })
    })()
    check('S5 客户端断开 → 门闩已清（再 prompt → 200 非 409）',
      r.aborted && after.status === 200 && after.body?.status !== undefined,
      `aborted=${r.aborted} after=${after.status}(${after.body?.status ?? after.body?.error})`)
  }

  // S8 turn 归属：断开重连后旧轮 turn/end 不收流（done 唯一且在第二轮 turn/end 之后）
  {
    const sessionId = await createSession(base)
    stub.handles.get(sessionId).mode = 'slow'
    const first = new AbortController()
    await streamOnce(base, sessionId, { prompt: '第一问' }, {
      signal: first.signal,
      onFrame: (f) => { if (f.event === 'delta' && !first.signal.aborted) first.abort() },
    })
    // 断开后旧轮仍在跑（1200ms 收尾），稍候（res close → clearBusy 的异步路径）
    // 再重连：新 prompt 排队为第二轮
    await sleep(250)
    const second = await streamOnce(base, sessionId, { prompt: '第二问' })
    const done = framesOf(second.frames, 'done')
    const turnEnds = framesOf(second.frames, 'turn/end')
    const secondRoundDelta = framesOf(second.frames, 'delta').find((f) => (f.json?.delta ?? '').includes('第2轮'))
    const doneIndex = second.frames.indexOf(done[0])
    check('S8 turn 归属：旧轮 turn/end 不误收流（done 唯一且在第二轮 delta/turn/end 之后）',
      second.status === 200 && done.length === 1 && turnEnds.length === 1 && secondRoundDelta !== undefined
        && second.frames.indexOf(secondRoundDelta) < doneIndex
        && turnEnds[0]?.json?.usage?.input === 11 && second.closed,
      `done=${done.length} turnEnd=${turnEnds.length} has2ndDelta=${secondRoundDelta !== undefined} closed=${second.closed}`)
  }

  // S9 会话级 error 事件：error 帧（真实错误信息）+ done 即时收流（不等看门狗）
  {
    const sessionId = await createSession(base)
    stub.handles.get(sessionId).mode = 'error'
    const t0 = Date.now()
    // stallTimeoutMs=5000：若 error 分支缺失，流要等 5s 才被看门狗以
    // "turn stalled" 误报收掉——elapsed 与 error 帧内容双重区分两条路径。
    const r = await streamOnce(base, sessionId, { prompt: '注定失败的一问', stallTimeoutMs: 5000 })
    const elapsed = Date.now() - t0
    const error = framesOf(r.frames, 'error')
    const done = framesOf(r.frames, 'done')
    check('S9 会话级 error 事件 → error 帧（真实错误信息）+ done 即时收流（不等看门狗）',
      r.status === 200 && error.length === 1
        && String(error[0]?.json?.message ?? '').includes('模型调用失败（注入错误）')
        && done.length === 1 && r.closed && elapsed < 4000,
      `error=${JSON.stringify(error[0]?.json)} done=${done.length} closed=${r.closed} elapsed=${elapsed}ms<5000ms`)
  }

  // S7 body 无 prompt → 400；未知 session → 404
  {
    const bad = await streamOnce(base, '00000000-0000-0000-0000-000000000001', { prompt: 'x' })
    const noPrompt = await streamOnce(base, '00000000-0000-0000-0000-000000000001', {})
    check('S7 未知 session → 404；body 无 prompt → 400',
      bad.status === 404 && bad.body?.error === 'not found' && noPrompt.status === 400,
      `unknown=${bad.status}(${JSON.stringify(bad.body)}) noPrompt=${noPrompt.status}`)
  }
}

// ---------------------------------------------------------------------------
// 真实 DSH 断言（DSH_BASE 可达时，Windows 端）
// ---------------------------------------------------------------------------

async function assertRealSuite(base) {
  // R1 正常流：connected / 逐 token delta（真流式：>1 帧，非 turn 结束后单帧补推）/ turn/end / done 后收流
  {
    const sessionId = await createSession(base)
    const r = await streamOnce(base, sessionId, { prompt: '数到5，每个数字一行', stallTimeoutMs: 300000 })
    const deltas = framesOf(r.frames, 'delta')
    const turnEnd = framesOf(r.frames, 'turn/end')
    const done = framesOf(r.frames, 'done')
    check('R1 真实流：connected + 逐 token delta（>1）+ turn/end + done 收流',
      r.status === 200 && framesOf(r.frames, 'connected').length === 1 && deltas.length > 1
        && turnEnd.length === 1 && done.length === 1 && r.closed,
      `status=${r.status} deltas=${deltas.length} turnEnd=${turnEnd.length} done=${done.length} closed=${r.closed}`)
  }
  // R2 忙时并发 → 409
  {
    const sessionId = await createSession(base)
    const first = streamOnce(base, sessionId, { prompt: '慢慢数到10', stallTimeoutMs: 300000 })
    await sleep(300)
    const second = await streamOnce(base, sessionId, { prompt: '并发第二问' })
    const firstResult = await first
    check('R2 真实忙时第二个 stream → 409',
      second.status === 409 && second.body?.error === 'session busy' && firstResult.closed,
      `second=${second.status} first closed=${firstResult.closed}`)
  }
  // R3/R4 400 与 404
  {
    const bad = await streamOnce(base, '00000000-0000-0000-0000-000000000001', { prompt: 'x' })
    const noPrompt = await streamOnce(base, '00000000-0000-0000-0000-000000000001', {})
    check('R3/R4 未知 session → 404；无 prompt → 400',
      bad.status === 404 && noPrompt.status === 400, `unknown=${bad.status} noPrompt=${noPrompt.status}`)
  }
  // R5 客户端断开 → 门闩已清
  {
    const sessionId = await createSession(base)
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), 1500)
    timer.unref?.()
    await streamOnce(base, sessionId, { prompt: '写一首一百字的诗', stallTimeoutMs: 300000 }, {
      signal: controller.signal,
    })
    // 与 S5 同理：稍候 res close → cleanup 异步路径
    await sleep(250)
    const after = await fetchJson(`${base}${PREFIX}/sessions/${sessionId}/prompt`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: '断开后再问', timeoutMs: 120000 }),
    })
    check('R5 真实客户端断开 → 门闩已清（再 prompt 非 409）',
      after.status === 200 && after.body?.error !== 'session busy',
      `after=${after.status}(${after.body?.status ?? after.body?.error})`)
  }
}

async function main() {
  console.log(`smoke-stream: DSH base = ${BASE}`)
  console.log('① sse.ts 基建直检')
  await assertSseInfra()
  console.log('② stub 冒烟（贴 0.2.2-rc.5 接口形状：delta 帧流 / turn/end / 心跳 / 409 / 门闩）')
  const port = 31981
  const stub = await startStubDsh(port)
  try {
    await assertStubSuite(`http://127.0.0.1:${port}`, stub)
  } finally {
    stub.cleanup()
  }
  if (await probe()) {
    console.log('③ DSH_BASE 可达：补跑真实 DSH 断言。')
    await assertRealSuite(BASE)
  } else {
    console.log(`③ DSH_BASE（${BASE}）不可达：真实 DSH 断言 SKIP（留 Windows 端），退出码 0。`)
  }
  const failed = results.filter((r) => !r.ok).length
  console.log(failed === 0 ? `\nALL PASS (${results.length}/${results.length})` : `\n${failed} FAILED`)
  process.exit(failed === 0 ? 0 : 1)
}

await main()
