#!/usr/bin/env node
/**
 * dsh-http-server 任务 5 冒烟：standalone 独立服务两栖挂载（双开验证）
 *
 * 两种运行模式（自动选择）：
 *
 * A. 真实 DSH 双开（DSH 机器上，宿主与 standalone 均已配置）：
 *    npx tsc
 *    # cordis.patch.yml 解开 config: {standalonePort: 3100} 注释（或经 profile patch 配置）后：
 *    npx @deepseek-ai/dsh web --patch ./cordis.patch.yml
 *    node scripts/smoke-standalone.mjs
 *    DSH 基址用 DSH_BASE（默认 http://127.0.0.1:3080），
 *    standalone 基址用 DSH_STANDALONE_BASE（默认 http://127.0.0.1:3100）。
 *    真实模式只断言双端口 health（brief 步骤 2 的三条 curl 前两条）；
 *    「卸载后两端口均关」需控制 DSH 生命周期，由人工重启验证。
 *
 * B. stub 双开（本机无 DSH 实例时的裁决路径）：DSH_BASE 不可达时，起一个
 *    stub 宿主——用 node:http 起真实 HTTP 服务模拟宿主 webServer（前缀路由表
 *    + 摘除语义贴 0.2.2-rc.5 dsh-host-webserver register 行为），加载编译产物
 *    lib/index.js 并调用其 apply(ctx, config)（config.standalonePort = 3100，
 *    显式传入而非改 cordis.patch.yml——裁决钉死），对同一进程内的两端口打
 *    真实 HTTP 请求。
 *
 * 断言（对应 brief 步骤 2；stub 模式全量，真实模式 SA1/SA2）：
 *   SA1 宿主端口 /api/v1/health → 200 {"ok":true,"service":"dsh-http-server",...}
 *   SA2 standalone 端口 /api/v1/health → 200（同上）
 *   SA3 两端口 health 响应体完全一致（共用同一 routeHandler/同一份路由状态）
 *   SA4 插件卸载（按注册逆序跑 effect 清理）→ standalone 端口不可达（ECONNREFUSED）；
 *       宿主端口存活但 health 路由已摘 → 404（宿主自身应答，非插件）
 *   SA5 宿主退出（close 宿主 server）→ 宿主端口亦不可达（两端口均关）
 *   SA6 standalone 端口被占（EADDRINUSE，审查 I-1）→ apply 不抛、宿主不受
 *       影响（health 照常 200）、错误经 ctx.logger 记录、unload 不挂（仅 stub）
 */

import { createServer } from 'node:http'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const BASE = process.env.DSH_BASE || 'http://127.0.0.1:3080'
const STANDALONE_BASE = process.env.DSH_STANDALONE_BASE || 'http://127.0.0.1:3100'
const HOST_PORT = Number(process.env.SMOKE_HOST_PORT || '3080')
const STANDALONE_PORT = Number(process.env.SMOKE_STANDALONE_PORT || '3100')
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

async function probe(base) {
  try {
    await fetch(base, { signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) })
    return true
  } catch (error) {
    const code = error?.cause?.code || error?.code || ''
    return !(code === 'UND_ERR_CONNECT_TIMEOUT' || code === 'ECONNREFUSED' || code === 'ENOTFOUND'
      || code === 'ETIMEDOUT' || error?.name === 'TimeoutError' || error?.name === 'AbortError')
  }
}

/** 端口是否已无人监听（ECONNREFUSED / 连接超时 → true）。 */
async function unreachable(base) {
  try {
    await fetch(base, { signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) })
    return false
  } catch (error) {
    const code = error?.cause?.code || error?.code || ''
    return code === 'ECONNREFUSED' || code === 'ENOTFOUND' || code === 'ETIMEDOUT'
      || error?.name === 'TimeoutError' || error?.name === 'AbortError' || code === 'UND_ERR_CONNECT_TIMEOUT'
  }
}

// ---------------------------------------------------------------------------
// stub 宿主：贴 0.2.2-rc.5 接口形状的最小 ctx。webServer 是前缀路由表
// （register 返回摘除 disposer，照 dsh-host-webserver lib/index.js 375-382 行），
// 宿主 HTTP 服务按「最长前缀命中→route.handler，未命中→宿主 404」分发。
// ---------------------------------------------------------------------------

/** stub sessionPersistence：本冒烟只打 health，实现为最小空账本。 */
function stubPersistence(root) {
  const sessions = new Map()
  return {
    root,
    stat: async (id) => (sessions.has(String(id)) ? { header: { cwd: sessions.get(String(id)).cwd } } : undefined),
    remember(sessionId, cwd) { sessions.set(sessionId, { cwd }) },
    forget(sessionId) { sessions.delete(sessionId) },
  }
}

/** stub AgentHandle / agents 服务（health 不触会话，仅保持 apply 装配可走通）。 */
function stubAgents() {
  return {
    async create() { throw new Error('stub: agents.create not expected in standalone smoke') },
    async resume() { throw new Error('stub: agents.resume not expected in standalone smoke') },
  }
}

/**
 * stub ctx：effect 立即执行 fn 并按 cordis 语义（注册逆序）收集 disposer；
 * on/emit 构成 session/event 总线；get 提供服务探测；webServer.register
 * 把前缀路由挂进宿主路由表并返回摘除函数。
 */
function makeStubCtx(persistence, hostRoutes) {
  const listeners = new Map()
  const disposers = []
  const logged = []
  return {
    ctxRef: null,
    get: (name) => (name === 'sessionPersistence' ? persistence : undefined),
    on: (name, cb) => {
      let set = listeners.get(name)
      if (set === undefined) { set = new Set(); listeners.set(name, set) }
      set.add(cb)
      return () => set.delete(cb)
    },
    emit: (name, ...args) => { for (const cb of [...(listeners.get(name) ?? [])]) cb(...args) },
    effect: (fn) => {
      const dispose = fn()
      disposers.unshift(dispose)
      return () => { const i = disposers.indexOf(dispose); if (i !== -1) disposers.splice(i, 1); void dispose?.() }
    },
    /** 按注册逆序执行全部 effect 清理（cordis「Disposers run in reverse registration order」）。 */
    async disposeAll() {
      while (disposers.length > 0) {
        const dispose = disposers.shift()
        if (typeof dispose === 'function') await dispose()
      }
    },
    agents: stubAgents(),
    llm: {
      listProviders: () => [{ id: 'stub-provider' }],
      listModels: async (provider) => [{ provider, id: 'stub-model' }],
    },
    /** logger stub（cordis LoggerService 最小面）：error 记录进 logged 供 SA6 断言。 */
    logger: {
      error: (...args) => { logged.push(args.map((a) => (a instanceof Error ? `${a.code ?? ''} ${a.message}` : String(a))).join(' ')) },
      warn: () => undefined,
      info: () => undefined,
      debug: () => undefined,
    },
    /** logger 记录的错误文本（暴露给 startStubDual → SA6 断言）。 */
    logged,
    webServer: {
      register(route) {
        hostRoutes.set(route.path, route)
        return () => { hostRoutes.delete(route.path) }
      },
    },
  }
}

/**
 * 起 stub 双开世界：宿主 HTTP 服务（HOST_PORT）+ 经 apply() 装配的插件
 * （config.standalonePort = STANDALONE_PORT 显式传入，裁决钉死不改
 * cordis.patch.yml 默认）。返回 { unload, shutdown, root, logged }。
 */
async function startStubDual() {
  const root = mkdtempSync(join(tmpdir(), 'dsh-smoke-standalone-'))
  const persistence = stubPersistence(root)
  const hostRoutes = new Map()
  const ctx = makeStubCtx(persistence, hostRoutes)

  // 宿主 webServer：前缀路由分发（最长前缀命中；未命中 → 宿主自身 404）
  const hostServer = createServer((req, res) => {
    const pathname = new URL(req.url ?? '/', 'http://x').pathname
    let matched = null
    for (const [path, route] of hostRoutes) {
      if (pathname.startsWith(path) && (matched === null || path.length > matched.path.length)) {
        matched = { path, route }
      }
    }
    if (matched === null) {
      res.statusCode = 404
      res.setHeader('Content-Type', 'application/json; charset=utf-8')
      res.end(JSON.stringify({ ok: false, error: `host: no route for ${pathname}` }))
      return
    }
    matched.route.handler(req, res)
  })
  await new Promise((done) => hostServer.listen(HOST_PORT, '127.0.0.1', done))

  // 插件装配：apply 是被测对象（routeHandler 两栖共用 + effect 生命周期）
  const plugin = await import(join(pluginDir, 'lib', 'index.js'))
  const config = {
    pathPrefix: PREFIX,
    standaloneHost: '127.0.0.1',
    standalonePort: STANDALONE_PORT,
    agentPreset: 'standard',
    workspaceRoot: root,
    stallTimeoutMs: 300000,
  }
  plugin.apply(ctx, config)

  return {
    root,
    /** 插件卸载：跑全部 effect 清理（standalone 关流 + 宿主路由摘除 + 会话清空 + 退订）。 */
    unload: () => ctx.disposeAll(),
    /** 宿主退出（DSH 退出模拟）：close 宿主 HTTP 服务。 */
    shutdown: () => new Promise((done) => hostServer.close(done)),
    cleanupRoot: () => rmSync(root, { recursive: true, force: true }),
    /** 经 ctx.logger.error 记录的错误文本（SA6 断言用；与 stub ctx 同一引用）。 */
    logged: ctx.logged,
  }
}

// ---------------------------------------------------------------------------
// 断言主体
// ---------------------------------------------------------------------------

async function assertHealthOk(name, base) {
  try {
    const { status, body } = await fetchJson(`${base}${PREFIX}/health`)
    check(name, status === 200 && body?.ok === true && body?.service === 'dsh-http-server'
      && typeof body?.version === 'string' && Array.isArray(body?.providers) && Array.isArray(body?.models),
      `status=${status} service=${body?.service} providers=${JSON.stringify(body?.providers)}`)
    return body
  } catch (error) {
    check(name, false, error?.message || String(error))
    return undefined
  }
}

async function assertStubSuite() {
  const world = await startStubDual()
  try {
    const host = `http://127.0.0.1:${HOST_PORT}`
    const standalone = `http://127.0.0.1:${STANDALONE_PORT}`

    // SA1/SA2：同进程两端口各发 health 均通（brief 步骤 2 前两条 curl）
    const hostBody = await assertHealthOk(`SA1 宿主 ${HOST_PORT} /api/v1/health → 200`, host)
    const standaloneBody = await assertHealthOk(`SA2 standalone ${STANDALONE_PORT} /api/v1/health → 200`, standalone)

    // SA3：两端口响应体完全一致（共用同一 routeHandler 的证据）
    check('SA3 两端口 health 响应体完全一致（共用同一 routeHandler）',
      hostBody !== undefined && standaloneBody !== undefined
        && JSON.stringify(hostBody) === JSON.stringify(standaloneBody),
      `host=${JSON.stringify(hostBody)} standalone=${JSON.stringify(standaloneBody)}`)

    // SA4：插件卸载 → standalone 端口关；宿主端口存活但路由已摘 → 404
    await world.unload()
    check(`SA4a 插件卸载后 standalone ${STANDALONE_PORT} 不可达（ECONNREFUSED）`,
      await unreachable(standalone), 'still reachable')
    try {
      const after = await fetchJson(`${host}${PREFIX}/health`)
      check(`SA4b 插件卸载后宿主 ${HOST_PORT} 存活但 health 路由已摘 → 404（宿主自身应答）`,
        after.status === 404, `status=${after.status} body=${after.text.slice(0, 80)}`)
    } catch (error) {
      check('SA4b 插件卸载后宿主存活但 health 路由已摘 → 404', false, error?.message || String(error))
    }

    // SA5：宿主退出 → 宿主端口亦不可达（brief「插件卸载/DSH 退出后两端口均不可达」）
    await world.shutdown()
    check(`SA5 宿主退出后 ${HOST_PORT} 不可达（两端口均关）`, await unreachable(host), 'still reachable')
  } finally {
    world.cleanupRoot()
  }
}

/**
 * SA6（审查 I-1）：standalone 端口被占 → listen 失败（EADDRINUSE）不得击穿
 * 宿主进程。流程：先占 STANDALONE_PORT → 起 stub 双开（apply 内 standalone
 * listen 撞占位者）→ 断言 apply 不抛、宿主 health 照常、错误经 ctx.logger
 * 记录、unload 不挂（对从未 listen 成功的 server close 是安全 no-op）。
 */
async function assertPortConflictSuite() {
  const squatter = createServer((_req, res) => { res.statusCode = 500; res.end('port squatter') })
  await new Promise((done) => squatter.listen(STANDALONE_PORT, '127.0.0.1', done))
  let world
  try {
    try {
      world = await startStubDual()
      check('SA6a standalone 端口被占（EADDRINUSE）：apply 不抛、宿主装配正常',
        true, 'startStubDual returned without throw')
    } catch (error) {
      check('SA6a standalone 端口被占（EADDRINUSE）：apply 不抛、宿主装配正常',
        false, error?.message || String(error))
      return
    }
    try {
      // listen 的 error 事件异步送达：稍候让 EADDRINUSE 走完 error 监听 + close 路径
      await sleep(150)
      // 进程未被 uncaught 'error' 击穿（走到这里即证据）+ 宿主照常服务
      const hostHealth = await fetchJson(`http://127.0.0.1:${HOST_PORT}${PREFIX}/health`)
      check(`SA6b 宿主不受影响（${HOST_PORT} health 照常 → 200，进程存活）`,
        hostHealth.status === 200 && hostHealth.body?.ok === true,
        `status=${hostHealth.status}`)
      const loggedText = (world.logged ?? []).join(' | ')
      check('SA6c listen 失败经 ctx.logger.error 记录（含 EADDRINUSE 与端口信息）',
        loggedText.includes('EADDRINUSE') && loggedText.includes(String(STANDALONE_PORT)),
        JSON.stringify(world.logged ?? []))
      // unload：standalone disposer 对从未 listen 成功的 server close 亦须收口（不挂）
      let unloaded = false
      await Promise.race([
        world.unload().then(() => { unloaded = true }),
        sleep(3000),
      ])
      check('SA6d listen 失败后 unload 不挂（disposer 幂等 close 正常收口）', unloaded === true, `unloaded=${unloaded}`)
      await world.shutdown()
    } finally {
      world.cleanupRoot()
    }
  } finally {
    await new Promise((done) => squatter.close(done))
  }
}

async function assertRealSuite() {
  await assertHealthOk(`SA1 真实宿主 ${BASE}/api/v1/health → 200`, BASE)
  if (await probe(STANDALONE_BASE)) {
    await assertHealthOk(`SA2 真实 standalone ${STANDALONE_BASE}/api/v1/health → 200`, STANDALONE_BASE)
  } else {
    console.log(`  SKIP  SA2 真实 standalone（${STANDALONE_BASE} 不可达：宿主未配置 standalonePort > 0）`)
  }
  console.log('  SKIP  SA3-SA6（真实模式无法在本脚本内控制 DSH 生命周期：卸载/退出/端口冲突由人工重启 DSH 验证）')
}

async function main() {
  console.log(`smoke-standalone: DSH base = ${BASE}, standalone base = ${STANDALONE_BASE}`)
  if (await probe(BASE)) {
    console.log('模式 A：对真实 DSH 实例断言（SA1/SA2；SA3-SA5 需控制 DSH 生命周期，人工验证）。')
    await assertRealSuite()
  } else {
    console.log(`模式 B（stub 双开）：本机 DSH 不可达，起 stub 宿主（${HOST_PORT}）+ apply 装配（standalone ${STANDALONE_PORT}）驱动 SA1-SA6。`)
    await assertStubSuite()
    await assertPortConflictSuite()
  }
  const failed = results.filter((r) => !r.ok).length
  console.log(failed === 0 ? `\nALL PASS (${results.length}/${results.length})` : `\n${failed} FAILED`)
  process.exit(failed === 0 ? 0 : 1)
}

await main()
