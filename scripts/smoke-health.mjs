#!/usr/bin/env node
/**
 * dsh-http-server 任务 1 冒烟：health 端点（宿主 webServer 挂载）
 *
 * 用法：先编译插件并在 DSH 实例上挂载（DSH 机器上执行）：
 *   npx tsc
 *   npx @deepseek-ai/dsh web --patch ./cordis.patch.yml
 *   node scripts/smoke-health.mjs
 *
 * DSH 基址可用 DSH_BASE 环境变量指向测试实例（默认 http://127.0.0.1:3080）。
 *
 * 断言（对应 brief 步骤 4）：
 *   1. GET  <base>/api/v1/health → 200 且 {"ok":true,"service":"dsh-http-server","version":"0.1.0"}
 *   2. GET  <base>/api/v1/nope   → 404 且 JSON 含 ok:false
 *
 * DSH 不可达时打印 SKIP 提示并退出码 0（本机无 DSH 实例；真实验证在用户的 Windows DSH 上执行）。
 */

const BASE = process.env.DSH_BASE || 'http://127.0.0.1:3080'
const PREFIX = '/api/v1'
const TIMEOUT_MS = 8000

const results = []

function check(name, ok, detail) {
  results.push({ name, ok })
  console.log(`${ok ? '  PASS' : '  FAIL'}  ${name}${detail ? ' — ' + detail : ''}`)
}

/** 带超时的 fetch（连接被拒/超时抛错） */
async function fetchWithTimeout(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(TIMEOUT_MS) })
  const text = await response.text()
  let body
  try { body = JSON.parse(text) } catch { body = undefined }
  return { status: response.status, text, body }
}

/** 探测 DSH 是否可达：连接被拒/ECONNREFUSED/DNS 失败/超时 → 不可达 */
async function probe() {
  try {
    await fetch(BASE, { signal: AbortSignal.timeout(TIMEOUT_MS) })
    // 任意 HTTP 响应（含 404）都算可达——只关心 TCP/HTTP 是否有人应答
    return true
  } catch (error) {
    const code = error?.cause?.code || error?.code || ''
    if (code === 'UND_ERR_CONNECT_TIMEOUT' || code === 'ECONNREFUSED' || code === 'ENOTFOUND' || code === 'ETIMEDOUT' || error?.name === 'TimeoutError' || error?.name === 'AbortError') {
      return false
    }
    // 其他错误（如非预期协议）也按不可达处理，打印原因供诊断
    console.log(`  probe: ${code || error?.name || 'error'} — ${error?.message || error}`)
    return false
  }
}

async function main() {
  console.log(`smoke-health: DSH base = ${BASE}`)

  if (!(await probe())) {
    console.log('SKIP: DSH 不可达（本机未运行 DSH 实例）。')
    console.log('      请在 DSH 机器上挂载插件后运行本脚本：DSH_BASE=http://127.0.0.1:3080 node scripts/smoke-health.mjs')
    process.exit(0)
  }

  // 1. health → 200 + 固定 JSON
  try {
    const { status, body } = await fetchWithTimeout(`${BASE}${PREFIX}/health`)
    const ok = status === 200
      && body?.ok === true
      && body?.service === 'dsh-http-server'
      && body?.version === '0.1.0'
    check('GET /api/v1/health → 200 {"ok":true,"service":"dsh-http-server","version":"0.1.0"}',
      ok, `status=${status}`)
  } catch (error) {
    check('GET /api/v1/health → 200', false, error?.message || String(error))
  }

  // 2. 未知路径 → 404 JSON
  try {
    const { status, body } = await fetchWithTimeout(`${BASE}${PREFIX}/nope`)
    const ok = status === 404 && body?.ok === false
    check('GET /api/v1/nope → 404 {"ok":false,...}', ok, `status=${status}`)
  } catch (error) {
    check('GET /api/v1/nope → 404', false, error?.message || String(error))
  }

  const failed = results.filter((r) => !r.ok).length
  console.log(failed === 0 ? '\nALL PASS' : `\n${failed} FAILED`)
  process.exit(failed === 0 ? 0 : 1)
}

await main()
