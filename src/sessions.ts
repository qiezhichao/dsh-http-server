/**
 * dsh-http-server — 进程内会话登记簿（SessionStore）
 *
 * 内存 Map<sessionId, Record> 管理本插件创建/恢复的 agent 句柄与忙闲门闩，
 * 配合 cwd 校验（workspaceRoot 沙箱）与跨重启恢复（sessionPersistence.stat 探测
 * → resumeAgent，参照官方 sdk server 600 行的 stat 用法）。
 *
 * 路由层只与本模块与 agent-adapter 交互；busy 门闩的语义是「同一 session 同时
 * 只允许一个在跑任务」（规格裁决，409 不排队）。
 */

import * as nodePath from 'node:path'
import type { PlatformPath } from 'node:path'
import type { Context } from '@deepseek-ai/cordis'
import type { AgentHandle } from '@deepseek-ai/dsh-agent'
import {
  createAgent,
  resolveRoute,
  resumeAgent,
  disposeHandle,
} from './agent-adapter.js'
import type { EventArchive } from './events.js'
import type { HttpServerConfig } from './config.js'
import type { CreateSessionBody } from './http-types.js'

/** 会话不存在（内存无 + 持久化 stat 无 + resume 失败）→ 路由层 404。 */
export class NotFoundError extends Error {
  constructor(sessionId: string) {
    super(`session not found: ${sessionId}`)
  }
}

/** cwd resolve 后逃出 workspaceRoot → 路由层 400。 */
export class CwdEscapeError extends Error {
  constructor(cwd: string, root: string) {
    super(`cwd escapes workspace root: ${cwd} (root: ${root})`)
  }
}

/** 一个在册会话：agent 句柄、resolve 后的 cwd、忙闲门闩。 */
export interface SessionRecord {
  handle: AgentHandle
  cwd: string
  busy: boolean
}

/** 会话登记簿：创建/恢复/忙闩/删除，与归档清理解耦。 */
export class SessionStore {
  private readonly records = new Map<string, SessionRecord>()

  constructor(
    private readonly ctx: Context,
    private readonly config: HttpServerConfig,
    private readonly archive: EventArchive,
    /** path 模块注入点：默认平台实现；win32 用例/未来测试可传 nodePath.win32 模拟。 */
    private readonly path: PlatformPath = nodePath,
  ) {}

  /**
   * cwd 沙箱校验：相对 workspaceRoot resolve（body.cwd 缺省 = 根本身），
   * resolve 后必须位于根下（含等于），否则 CwdEscapeError。
   * 前缀比较用 path.sep 而非硬编码 '/'（win32 resolve 产物是 '\\' 分隔），
   * resolve 已折叠全部 '..'，前缀命中即必在根下。
   * 返回 resolve 后的绝对路径（create 的 meta.cwd 用它，持久化 header 亦固化它）。
   */
  private validateCwd(raw: string | undefined): string {
    const base = this.path.resolve(this.config.workspaceRoot)
    const target = this.path.resolve(base, raw ?? '.')
    if (target !== base && !target.startsWith(base + this.path.sep)) {
      throw new CwdEscapeError(raw ?? '.', base)
    }
    return target
  }

  /**
   * 取在册会话；不在册时的语义（brief 步骤 3 逐字）：
   *  - sessionId === undefined → 新建（进程内新 UUID，createAgent）
   *  - 不存在但可恢复（sessionPersistence.stat 命中）→ resumeAgent
   *  - 不存在且不可恢复（stat 失败/未命中、无持久化服务）→ NotFoundError
   * cwd 校验先行（新建与 resume 前都校验——resume 时 body.cwd 缺省沿用持久化
   * header 的 cwd；显式给出时校验并以其为准）。
   */
  async getOrCreate(sessionId: string | undefined, body: CreateSessionBody): Promise<SessionRecord> {
    if (sessionId === undefined) {
      const cwd = this.validateCwd(body.cwd)
      const route = resolveRoute(this.ctx, this.config, body)
      const handle = await createAgent(this.ctx, route, cwd, this.config.agentPreset)
      const record: SessionRecord = { handle, cwd, busy: false }
      this.records.set(String(handle.agent.session.id), record)
      return record
    }
    const existing = this.records.get(sessionId)
    if (existing !== undefined) return existing
    // 跨重启恢复：先持久化 stat 探测（官方 sdk server 600 行同款），
    // 未命中/服务缺失/stat 抛错一律 NotFound（路由层 404）。
    let snapshot: { header: { cwd?: string } } | undefined
    try {
      snapshot = await this.ctx.get('sessionPersistence')?.stat(sessionId as never)
    } catch {
      snapshot = undefined
    }
    if (snapshot === undefined) throw new NotFoundError(sessionId)
    // cwd：显式给出则校验覆盖；缺省沿用持久化 header 固化的 cwd。
    const cwd = body.cwd === undefined
      ? (snapshot.header.cwd ?? this.validateCwd(undefined))
      : this.validateCwd(body.cwd)
    const route = resolveRoute(this.ctx, this.config, body)
    let handle: AgentHandle
    try {
      handle = await resumeAgent(this.ctx, sessionId, route, this.config.agentPreset)
    } catch {
      throw new NotFoundError(sessionId)
    }
    const record: SessionRecord = { handle, cwd, busy: false }
    this.records.set(sessionId, record)
    return record
  }

  /** 在册会话（不触发恢复；路由层先 probe 再动门闩用）。 */
  get(sessionId: string): SessionRecord | undefined {
    return this.records.get(sessionId)
  }

  /** 忙闩查询：不在册视为不忙（409 判定前路由层已确保在册）。 */
  isBusy(sessionId: string): boolean {
    return this.records.get(sessionId)?.busy ?? false
  }

  /** 置忙。 */
  setBusy(sessionId: string): void {
    const record = this.records.get(sessionId)
    if (record !== undefined) record.busy = true
  }

  /** 清忙。 */
  clearBusy(sessionId: string): void {
    const record = this.records.get(sessionId)
    if (record !== undefined) record.busy = false
  }

  /** 删除会话：dispose 句柄 + 注销 + 清归档。不存在时静默（DELETE 幂等）。 */
  async delete(sessionId: string): Promise<void> {
    const record = this.records.get(sessionId)
    if (record === undefined) return
    this.records.delete(sessionId)
    try {
      await disposeHandle(record.handle)
    } finally {
      this.archive.clear(sessionId)
    }
  }

  /** 插件卸载时清空全部句柄（index.ts 装进 ctx.effect 清理）。 */
  async disposeAll(): Promise<void> {
    const records = [...this.records.values()]
    this.records.clear()
    for (const record of records) {
      try {
        await disposeHandle(record.handle)
      } catch { /* 卸载路径尽力清 */ }
    }
    for (const sessionId of this.archive.sessionIds()) this.archive.clear(sessionId)
  }
}
