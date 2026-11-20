#!/usr/bin/env node
/**
 * dsh-http-server 任务 3 冒烟：sessions/prompt/tasks/events/wait/delete 路由
 *
 * 两种运行模式（自动选择）：
 *
 * A. 真实 DSH 实例（DSH 机器上，先挂插件）：
 *    npx tsc
 *    npx @deepseek-ai/dsh web --patch ./cordis.patch.yml
 *    node scripts/smoke-sessions.mjs
 *    DSH 基址用 DSH_BASE 指向（默认 http://127.0.0.1:3080）。
 *
 * B. stub 冒烟（本机无 DSH 实例时的裁决路径）：脚本顶部探测 DSH_BASE 不可达
 *    时，不直接退出，而是起一个 stub DSH——用 node:http 起真实 HTTP 服务、
 *    加载编译产物 lib/router.js + stub ctx（agent handle/事件流/persistence 全部
 *    贴 0.2.2-rc.5 真实接口形状，依据 node_modules 与 /tmp/dsh-research/repo），
 *    对同一组 7 项断言打真实 HTTP 请求。
 *
 * 断言（对应 brief 步骤 5 + 审查修复回归 3 项）：
 *   1. POST /sessions → 201 有 sessionId
 *   2. POST prompt {prompt, timeoutMs:60000} → 200 status=completed，text 非空
 *   3. 忙时并发第二个 prompt → 409
 *   4. GET events?since=0 → 有 turn/end 事件与 usage
 *   5. wait → 200
 *   6. DELETE → 204；再 prompt → 404（无持久化会话时）
 *   7. cwd 逃逸 → 400
 *   8. (a 审查 C-1) ?since=<cursor> 只返回 seq>cursor 的事件（stub 下 cursor=3）
 *   9. (b 审查 I-1) tasks followup 抛错后忙闩已清——再发 prompt 不 409（仅 stub 可注入）
 *  10. (c 审查 I-2) win32 模拟：合法子目录 cwd 201、..\ 逃逸 400（仅 stub 可模拟）
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

/** 带超时的 fetch */
async function fetchJson(url, options) {
  const response = await fetch(url, { ...options, signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) })
  const text = await response.text()
  let body
  try { body = JSON.parse(text) } catch { body = undefined }
  return { status: response.status, text, body }
}

/** 探测 DSH 是否可达 */
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

// ---------------------------------------------------------------------------
// stub DSH：贴 0.2.2-rc.5 真实接口形状的最小 ctx / agents / sessionPersistence。
// 依据：node_modules/@deepseek-ai/dsh-agent/lib/types/*.d.ts（AgentHandle/
// Agent.followup/whenIdle/status/session.header）、dsh-session SessionEvent
// {type,seq,time,data}、SessionPersistence.stat → {header:{cwd,...}}。
// ---------------------------------------------------------------------------

/** 0.2.2-rc.5 事件形状：SessionEvent = {type, seq, time, data, ...}。 */
function event(type, seq, data) {
  return { type, seq, time: Date.now(), data }
}

/** stub 一个 AgentHandle：followup 触发一轮 turn 生命周期事件 + whenIdle 解锁。 */
function stubHandle(sessionId, persistence) {
  const events = []
  let seq = persistence.startSeqOf(sessionId)
  let idle
  const handle = {
    agent: {
      session: {
        id: sessionId,
        header: { cwd: persistence.cwdOf(sessionId) },
        get seq() { return seq },
      },
      status: 'idle',
      followup(message) {
        // 故障注入开关（断言 9 用）：置 true 后下一次 followup 同步抛错且不动状态。
        if (handle.failNextFollowup) {
          handle.failNextFollowup = false
          throw new Error('stub followup failure')
        }
        handle.agent.status = 'running'
        const turn = persistence.nextTurnOf(sessionId)
        const push = (e) => { events.push(e); persistence.recordEvent(sessionId, e) }
        push(event('turn/start', seq++, { turn }))
        push(event('user/message', seq++, {
          turn, step: 1,
          message: { role: 'user', id: message.id, content: [{ type: 'text', text: message.content[0].text }], source: { kind: 'user' } },
        }))
        // 模拟一段耗时后 assistant 全文 + turn/end（官方事件顺序）。
        const settle = () => {
          const text = `你好，我是 DSH stub 会话 ${sessionId.slice(0, 8)} 的模型回复。`
          push(event('assistant/message', seq++, {
            turn, step: 1,
            message: { role: 'assistant', id: `m-${seq}`, content: [{ type: 'text', text }], source: { kind: 'model' } },
            stream: [],
            usage: { inputTokens: 12, outputTokens: 34, totalTokens: 46, cacheReadTokens: 5, cacheWriteTokens: 0 },
          }))
          push(event('turn/end', seq++, { turn, reason: { kind: 'completed' } }))
          handle.agent.status = 'idle'
          persistence.setUsage(sessionId, { input: 12, output: 34, cacheRead: 5, cacheWrite: 0 })
          if (idle !== undefined) { const w = idle; idle = undefined; w() }
        }
        setTimeout(settle, 60)
      },
      whenIdle() {
        if (handle.agent.status === 'idle') return Promise.resolve()
        return new Promise((w) => { idle = w })
      },
    },
    async dispose() {
      persistence.forget(sessionId)
      handle.disposed = true
    },
    disposed: false,
    /** 故障注入开关（断言 9 用，见 followup 头注释）。 */
    failNextFollowup: false,
  }
  return handle
}

/** stub sessionPersistence：stat 返回 {header:{cwd}} 或 undefined（0.2.2-rc.5 形状）。 */
function stubPersistence(root) {
  const sessions = new Map()
  return {
    root,
    stat: async (id) => {
      const s = sessions.get(String(id))
      return s === undefined ? undefined : { header: { cwd: s.cwd }, revision: 'rev', eventCount: s.eventCount }
    },
    remember(sessionId, cwd) { sessions.set(sessionId, { cwd, eventCount: 0, turn: 0, startSeq: 0, usage: undefined }) },
    forget(sessionId) { sessions.delete(sessionId) },
    has(sessionId) { return sessions.has(sessionId) },
    cwdOf(sessionId) { return sessions.get(sessionId)?.cwd },
    startSeqOf(sessionId) { return sessions.get(sessionId)?.startSeq ?? 0 },
    nextTurnOf(sessionId) { const s = sessions.get(sessionId); s.turn += 1; return s.turn },
    recordEvent(sessionId, e) { const s = sessions.get(sessionId); s.eventCount += 1 },
    setUsage(sessionId, usage) { sessions.get(sessionId).usage = usage },
  }
}

/** stub ctx（主 stub 与 win32 stub 共用）：sessionPersistence + agents 服务。 */
function makeStubCtx(persistence, root, handles) {
  return {
    get: (name) => (name === 'sessionPersistence' ? persistence : undefined),
    // resolveRoute 走 ctx.get('agentDefaultModel')，stub 无该服务 → 全空回退 config。
    agents: {
      async create(options) {
        const sessionId = String(options.sessionId)
        persistence.remember(sessionId, options.meta?.cwd ?? root)
        const handle = stubHandle(sessionId, persistence)
        handles.set(sessionId, handle)
        return handle
      },
      async resume(options) {
        const sessionId = String(options.resumeSessionId)
        if (!persistence.has(sessionId)) throw new Error('not persisted')
        const handle = stubHandle(sessionId, persistence)
        handles.set(sessionId, handle)
        return handle
      },
    },
  }
}

/** 起 HTTP 服务挂 match 分发（端口监听 + cleanup），主/win32 stub 共用骨架。 */
async function listenRouter(port, deps) {
  const router = await import(join(pluginDir, 'lib', 'router.js'))
  const server = createServer((req, res) => {
    router.match(req, res, router.stripPrefix(router.requestPath(req), PREFIX), deps)
  })
  await new Promise((done) => server.listen(port, '127.0.0.1', done))
  return server
}

/** 起 stub DSH：加载编译产物 lib/{router,events,sessions,agent-adapter}.js。 */
async function startStubDsh(port) {
  const root = mkdtempSync(join(tmpdir(), 'dsh-smoke-ws-'))
  const persistence = stubPersistence(root)
  const { EventArchive } = await import(join(pluginDir, 'lib', 'events.js'))
  const { SessionStore } = await import(join(pluginDir, 'lib', 'sessions.js'))
  const handles = new Map()
  const ctx = makeStubCtx(persistence, root, handles)
  const config = {
    pathPrefix: '/api/v1', standaloneHost: '127.0.0.1', standalonePort: 0,
    agentPreset: 'standard', workspaceRoot: root, stallTimeoutMs: 300000,
  }
  const archive = new EventArchive()
  // path 模块取平台默认（macOS/Linux 为 POSIX）——I-2 的 win32 用例走 startWin32Stub。
  const store = new SessionStore(ctx, config, archive)
  // 模拟 session/event 订阅：stub followup 产事件时同步 mirror 进 archive
  // （真装配在 index.ts 用 ctx.on('session/event') + 在册过滤，此处等效直推）。
  persistence.recordEvent = (sessionId, e) => { archive.append(sessionId, e) }
  const server = await listenRouter(port, { ctx, config, store, archive })
  return {
    server, root, persistence, handles,
    cleanup: () => { server.close(); rmSync(root, { recursive: true, force: true }) },
  }
}

/** 起 win32 模拟 stub（审查 I-2 回归）：SessionStore 注入 nodePath.win32，workspaceRoot 用 win32 路径。 */
async function startWin32Stub(port) {
  const nodePath = await import('node:path')
  const persistence = stubPersistence('C:\\dsh-ws')
  const { EventArchive } = await import(join(pluginDir, 'lib', 'events.js'))
  const { SessionStore } = await import(join(pluginDir, 'lib', 'sessions.js'))
  const handles = new Map()
  const ctx = makeStubCtx(persistence, 'C:\\dsh-ws', handles)
  const config = {
    pathPrefix: '/api/v1', standaloneHost: '127.0.0.1', standalonePort: 0,
    agentPreset: 'standard', workspaceRoot: 'C:\\dsh-ws', stallTimeoutMs: 300000,
  }
  const archive = new EventArchive()
  const store = new SessionStore(ctx, config, archive, nodePath.win32)
  persistence.recordEvent = (sessionId, e) => { archive.append(sessionId, e) }
  const server = await listenRouter(port, { ctx, config, store, archive })
  return { server, cleanup: () => server.close() }
}

// ---------------------------------------------------------------------------
// 断言主体（真实 DSH 与 stub 共用；base 指向被测端点）
// ---------------------------------------------------------------------------

async function assertAll(base) {
  // 1. POST /sessions → 201 有 sessionId
  let sessionId
  try {
    const { status, body } = await fetchJson(`${base}${PREFIX}/sessions`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })
    sessionId = body?.sessionId
    check('1. POST /sessions → 201 {sessionId, cwd}', status === 201 && typeof body?.sessionId === 'string' && body.sessionId.length > 0,
      `status=${status} sessionId=${body?.sessionId ?? 'none'}`)
  } catch (error) {
    check('1. POST /sessions → 201', false, error?.message || String(error))
    return
  }

  // 2. POST prompt → 200 status=completed，text 非空（第二个 prompt 与本条并发制造忙场景）
  let prompt2
  const prompt1 = fetchJson(`${base}${PREFIX}/sessions/${sessionId}/prompt`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ prompt: '用一句话自我介绍', timeoutMs: 60000 }),
  }).then((r) => { check('2. POST prompt → 200 status=completed text 非空',
    r.status === 200 && r.body?.status === 'completed' && typeof r.body?.text === 'string' && r.body.text.length > 0,
    `status=${r.status} body.status=${r.body?.status} text=${JSON.stringify((r.body?.text ?? '').slice(0, 40))}`); return r })

  // 3. 忙时并发第二个 prompt → 409（prompt1 进行中立发）
  try {
    prompt2 = await fetchJson(`${base}${PREFIX}/sessions/${sessionId}/prompt`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: '并发第二问', timeoutMs: 60000 }),
    })
    // 409 与 200 都可能：若 stub/真实 DSH 第一问已瞬完，这里记录实际值再判。
    check('3. 忙时并发第二个 prompt → 409', prompt2.status === 409 && prompt2.body?.error === 'session busy',
      `status=${prompt2.status} body=${JSON.stringify(prompt2.body)}`)
  } catch (error) {
    check('3. 忙时并发第二个 prompt → 409', false, error?.message || String(error))
  }
  await prompt1

  // 4. GET events?since=0 → 有 turn/end 事件与 usage；记录 cursor 供断言 8
  let cursor = 0
  try {
    const { status, body } = await fetchJson(`${base}${PREFIX}/sessions/${sessionId}/events?since=0`)
    const types = (body?.events ?? []).map((e) => e.type)
    const hasTurnEnd = types.includes('turn/end')
    const usage = body?.events?.find((e) => e.type === 'assistant/message')?.data?.usage
    if (typeof body?.nextSince === 'number') cursor = body.nextSince
    check('4. GET events?since=0 → 含 turn/end 与 usage',
      status === 200 && hasTurnEnd && usage !== undefined && body?.nextSince >= 0,
      `status=${status} events=${types.join(',')} usage=${usage ? 'present' : 'missing'} cursor=${cursor}`)
  } catch (error) {
    check('4. GET events?since=0 → 含 turn/end 与 usage', false, error?.message || String(error))
  }

  // 8. (a 审查 C-1) ?since=<cursor> 只返回 seq>cursor 的事件。
  //    修复前 since 参数被 requestPath 剥掉恒解析为 0 → 返回全部旧事件（含 seq≤cursor）→ FAIL。
  try {
    const again = await fetchJson(`${base}${PREFIX}/sessions/${sessionId}/prompt`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: '第二问', timeoutMs: 60000 }),
    })
    const ev = await fetchJson(`${base}${PREFIX}/sessions/${sessionId}/events?since=${cursor}`)
    const evs = ev.body?.events ?? []
    const seqs = evs.map((e) => e.seq)
    check(`8. (a) ?since=${cursor} 只返回 seq>${cursor} 的事件`,
      ev.status === 200 && again.status === 200 && evs.length > 0 && seqs.every((s) => s > cursor),
      `第二轮 prompt=${again.status} count=${evs.length} seq=[${seqs.join(',')}@..] nextSince=${ev.body?.nextSince}`)
  } catch (error) {
    check(`8. (a) ?since=${cursor} 只返回 seq>${cursor} 的事件`, false, error?.message || String(error))
  }

  // 5. wait → 200
  try {
    const { status, body } = await fetchJson(`${base}${PREFIX}/sessions/${sessionId}/wait`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ timeoutMs: 60000 }),
    })
    check('5. POST wait → 200', status === 200 && (body?.status === 'completed' || body?.status === 'timeout'),
      `status=${status} body.status=${body?.status}`)
  } catch (error) {
    check('5. POST wait → 200', false, error?.message || String(error))
  }

  // 6. DELETE → 204；再 prompt → 404（无持久化会话时）
  try {
    const del = await fetchJson(`${base}${PREFIX}/sessions/${sessionId}`, { method: 'DELETE' })
    const after = await fetchJson(`${base}${PREFIX}/sessions/${sessionId}/prompt`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: '删后再问', timeoutMs: 60000 }),
    })
    check('6. DELETE → 204；再 prompt → 404', del.status === 204 && after.status === 404 && after.body?.error === 'not found',
      `delete=${del.status} after=${after.status} body=${JSON.stringify(after.body)}`)
  } catch (error) {
    check('6. DELETE → 204；再 prompt → 404', false, error?.message || String(error))
  }

  // 7. cwd 逃逸 → 400
  try {
    const { status, body } = await fetchJson(`${base}${PREFIX}/sessions`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cwd: '../../escape' }),
    })
    check('7. cwd 逃逸 → 400 {"error":"invalid cwd"}', status === 400 && body?.error === 'invalid cwd',
      `status=${status} body=${JSON.stringify(body)}`)
  } catch (error) {
    check('7. cwd 逃逸 → 400', false, error?.message || String(error))
  }
}

/** 9. (b 审查 I-1) tasks followup 抛错后忙闩已清——再发 prompt 不 409（仅 stub：需句柄故障注入）。 */
async function assertTasksBusyLatch(base, stub) {
  try {
    const created = await fetchJson(`${base}${PREFIX}/sessions`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })
    const s2 = created.body?.sessionId
    const handle = stub.handles.get(s2)
    handle.failNextFollowup = true
    const task = await fetchJson(`${base}${PREFIX}/sessions/${s2}/tasks`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: '注入故障' }),
    })
    const after = await fetchJson(`${base}${PREFIX}/sessions/${s2}/prompt`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt: '故障后再问', timeoutMs: 60000 }),
    })
    check('9. (b) tasks followup 抛错后忙闩已清（再发 prompt 不 409）',
      task.status === 500 && after.status === 200 && after.body?.status === 'completed',
      `tasks=${task.status}(${JSON.stringify(task.body?.error ?? '')}) after=${after.status}(${after.body?.status ?? after.body?.error})`)
  } catch (error) {
    check('9. (b) tasks followup 抛错后忙闩已清', false, error?.message || String(error))
  }
}

/** 10. (c 审查 I-2) win32 模拟：合法子目录 cwd 201、..\ 逃逸 400（仅 stub：需注入 nodePath.win32）。 */
async function assertWin32Cwd(base) {
  try {
    const sub = await fetchJson(`${base}${PREFIX}/sessions`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cwd: 'sub\\dir' }),
    })
    const escape = await fetchJson(`${base}${PREFIX}/sessions`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cwd: '..\\..\\windows' }),
    })
    check('10. (c) win32 模拟：合法子目录 201 / ..\\ 逃逸 400',
      sub.status === 201 && typeof sub.body?.sessionId === 'string'
        && escape.status === 400 && escape.body?.error === 'invalid cwd',
      `sub=${sub.status}(cwd=${JSON.stringify(sub.body?.cwd ?? '')}) escape=${escape.status}(${JSON.stringify(escape.body ?? '')})`)
  } catch (error) {
    check('10. (c) win32 模拟：合法子目录 201 / ..\\ 逃逸 400', false, error?.message || String(error))
  }
}

async function main() {
  console.log(`smoke-sessions: DSH base = ${BASE}`)
  if (await probe()) {
    console.log('模式 A：对真实 DSH 实例断言（1-8 项；9/10 需 stub 注入，真实实例 SKIP）。')
    await assertAll(BASE)
  } else {
    console.log('模式 B（stub）：本机 DSH 不可达，起 stub DSH（贴 0.2.2-rc.5 接口形状）驱动 10 项断言。')
    const port = 31971
    const stub = await startStubDsh(port)
    const win32 = await startWin32Stub(port + 1)
    try {
      await assertAll(`http://127.0.0.1:${port}`)
      await assertTasksBusyLatch(`http://127.0.0.1:${port}`, stub)
      await assertWin32Cwd(`http://127.0.0.1:${port + 1}`)
    } finally {
      stub.cleanup()
      win32.cleanup()
    }
  }
  const failed = results.filter((r) => !r.ok).length
  console.log(failed === 0 ? `\nALL PASS (${results.length}/${results.length})` : `\n${failed} FAILED`)
  process.exit(failed === 0 ? 0 : 1)
}

await main()
