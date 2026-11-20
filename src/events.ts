/**
 * dsh-http-server — 会话事件归档
 *
 * 订阅 `session/event`（装配在 index.ts），把每个 agent 会话的事件按
 * { type, seq, time, data } 原样归档进内存，供 GET /sessions/:id/events 增量轮询
 * 与 statusOf/lastAssistantTextOf/usageOf 派生查询消费。
 *
 * 语义边界：
 *  - 归档是进程内增量的：跨重启 resume 后从恢复点开始积累，持久化历史不回填
 *    （本期规格裁决，历史轨迹后续从 DSH 持久化存储补）。
 *  - 环形截断：单会话超 10000 条丢最旧（seq 仍单调递增，since 游标不受丢旧影响）。
 *  - 事件对象是 DSH append 后的已冻结快照，归档只保留引用不拷贝（append-only、
 *    深冻结，保留引用即安全；避免 10000 条上限下的常驻拷贝开销）。
 */

import type { AgentHandle } from '@deepseek-ai/dsh-agent'
import type { Session, SessionEvent } from '@deepseek-ai/dsh-session'
import type { Context } from '@deepseek-ai/cordis'
import type { SessionEventView, TokenUsage } from './http-types.js'

/** 单会话归档上限，超出丢最旧（规格「我替你做的决定」第 9 条）。 */
const ARCHIVE_LIMIT = 10000

/** turn/end 的 error reason 提取（reason.kind === 'error' 时的 message，unknown 安全）。 */
function turnEndErrorOf(data: unknown): string | undefined {
  if (typeof data !== 'object' || data === null) return undefined
  const reason = (data as { reason?: unknown }).reason
  if (typeof reason !== 'object' || reason === null) return undefined
  if ((reason as { kind?: unknown }).kind !== 'error') return undefined
  const error = (reason as { error?: unknown }).error
  if (typeof error === 'string') return error
  if (typeof error === 'object' && error !== null) {
    const message = (error as { message?: unknown }).message
    if (typeof message === 'string') return message
  }
  const message = (reason as { message?: unknown }).message
  return typeof message === 'string' ? message : 'turn ended with error'
}

/** 一个会话的归档条目（SessionEvent 的视图投影）。 */
interface Entry {
  view: SessionEventView
  /** 最后一个 turn/end 携带的 usage（无则 undefined）。 */
  lastUsage?: TokenUsage
  /** 最后一条 assistant/message 的文本内容。 */
  lastAssistantText: string
  /**
   * 最近一个 turn/end 的 error 信息（reason.kind === 'error' 时有值；正常收
   * 尾的 turn/end 清空）。滚动语义与 lastUsage 一致：取"最新 turn 的收尾"。
   */
  lastTurnError?: string
}

/** DSH TokenUsage（inputTokens/outputTokens/cacheReadTokens/cacheWriteTokens，可缺省）→ HTTP 视图。 */
function toUsage(data: unknown): TokenUsage | undefined {
  if (typeof data !== 'object' || data === null) return undefined
  const usage = (data as { usage?: unknown }).usage
  if (typeof usage !== 'object' || usage === null) return undefined
  const { inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens } = usage as {
    inputTokens?: number; outputTokens?: number; cacheReadTokens?: number; cacheWriteTokens?: number
  }
  return {
    input: inputTokens ?? 0,
    output: outputTokens ?? 0,
    cacheRead: cacheReadTokens ?? 0,
    cacheWrite: cacheWriteTokens ?? 0,
  }
}

/** assistant/message 的 message.content 文本块拼接（对齐 dsh-llm extractTextContent 语义）。 */
function textOf(message: unknown): string {
  if (typeof message !== 'object' || message === null) return ''
  const content = (message as { content?: unknown }).content
  if (!Array.isArray(content)) return ''
  let text = ''
  for (const block of content) {
    if (typeof block === 'object' && block !== null
      && (block as { type?: unknown }).type === 'text'
      && typeof (block as { text?: unknown }).text === 'string') {
      text += (block as { text: string }).text
    }
  }
  return text
}

/** 运行中判定：归档里存在 turn/start 无配对 turn/end，或句柄 status==='running'。 */
function runningFromEntries(entries: readonly Entry[], handle: AgentHandle | undefined): boolean {
  if (handle !== undefined && handle.agent.status === 'running') return true
  let open = false
  for (const entry of entries) {
    if (entry.view.type === 'turn/start') open = true
    else if (entry.view.type === 'turn/end') open = false
  }
  return open
}

/** 内存事件归档：sessionId → 有序条目列表（append 顺序即 seq 顺序）。 */
export class EventArchive {
  private readonly sessions = new Map<string, Entry[]>()

  /** session/event 订阅回调（index.ts 装配）。归档条目并滚动派生字段。 */
  append(sessionId: string, event: SessionEvent): void {
    let entries = this.sessions.get(sessionId)
    if (entries === undefined) {
      entries = []
      this.sessions.set(sessionId, entries)
    }
    const last = entries[entries.length - 1]
    // 0.2.2-rc.5 真实形状：usage 挂在 assistant/message.data.usage（turn/end 无 usage
    // 字段），滚动保留到条目上；turn/end 若将来携带 usage 也覆盖（双兼容）。
    const usageFromThis = event.type === 'assistant/message' || event.type === 'turn/end'
      ? toUsage(event.data)
      : undefined
    const entry: Entry = {
      view: { type: event.type, seq: event.seq, time: event.time, data: event.data },
      lastUsage: usageFromThis ?? last?.lastUsage,
      lastAssistantText: event.type === 'assistant/message'
        ? textOf((event.data as { message?: unknown }).message)
        : (last?.lastAssistantText ?? ''),
      // turn/end 更新错误收尾（error reason 设值、正常收尾清空）；其他事件
      // 沿用 last（新 turn 开始时不预清——以该 turn 自己的收尾为准）。
      lastTurnError: event.type === 'turn/end'
        ? turnEndErrorOf(event.data)
        : last?.lastTurnError,
    }
    entries.push(entry)
    // 环形截断丢最旧时，滚动派生字段随 last 条目保留（usage/text 不受丢旧影响）。
    if (entries.length > ARCHIVE_LIMIT) entries.shift()
  }

  /** seq > since 的事件视图（丢最旧不影响游标：seq 单调）。无归档的会话返回空。 */
  since(sessionId: string, since: number): SessionEventView[] {
    const entries = this.sessions.get(sessionId)
    if (entries === undefined) return []
    const events: SessionEventView[] = []
    for (const entry of entries) {
      if (entry.view.seq > since) events.push(entry.view)
    }
    return events
  }

  /** 会话运行状态（turn 边界 + 句柄 status 双源判定）。 */
  statusOf(sessionId: string, handle: AgentHandle | undefined): 'idle' | 'running' {
    return runningFromEntries(this.sessions.get(sessionId) ?? [], handle) ? 'running' : 'idle'
  }

  /** 最后一条 assistant/message 文本。 */
  lastAssistantTextOf(sessionId: string): string {
    const entries = this.sessions.get(sessionId)
    return entries === undefined || entries.length === 0 ? '' : (entries[entries.length - 1]?.lastAssistantText ?? '')
  }

  /** 最后一个 turn/end 的 usage。 */
  usageOf(sessionId: string): TokenUsage | undefined {
    const entries = this.sessions.get(sessionId)
    return entries === undefined || entries.length === 0 ? undefined : entries[entries.length - 1]?.lastUsage
  }

  /**
   * 最近一个 turn/end 的错误信息（正常收尾为 undefined）。
   * chatResultOf 消费：agent 转入 idle 但最新 turn 以 error 收尾时，
   * prompt/wait/tasks 返回 status:'error' 而非 completed + 空文本。
   */
  lastTurnErrorOf(sessionId: string): string | undefined {
    const entries = this.sessions.get(sessionId)
    return entries === undefined || entries.length === 0 ? undefined : entries[entries.length - 1]?.lastTurnError
  }

  /** 归档里该会话最新的 turn 序号（tasks 路由的 taskId 后缀；无归档为 0）。 */
  lastTurnOf(sessionId: string): number {
    const entries = this.sessions.get(sessionId)
    if (entries === undefined) return 0
    let turn = 0
    for (const entry of entries) {
      if (entry.view.type === 'turn/start' || entry.view.type === 'turn/end') {
        const value = (entry.view.data as { turn?: unknown }).turn
        if (typeof value === 'number') turn = value
      }
    }
    return turn
  }

  /** DELETE 会话时清归档（事件流已断，无需保留）。 */
  clear(sessionId: string): void {
    this.sessions.delete(sessionId)
  }

  /** 全部有归档的 sessionId（disposeAll 清理用）。 */
  sessionIds(): string[] {
    return [...this.sessions.keys()]
  }
}

/**
 * 装配 `session/event` 订阅：只归档 isArchived 认可的会话（index.ts 传入
 * 「本插件 SessionStore 在册」判定）。宿主其他来源的会话（如 web UI 自建的）
 * 事件直接忽略——否则外全会话永不清理，长期运行无上限驻留（审查 I-3）。
 * 返回 effect 清理函数（ctx.on 返回 disposer，装进 ctx.effect 供卸载时退订）。
 */
export function attachEventArchive(
  ctx: Context,
  archive: EventArchive,
  isArchived: (sessionId: string) => boolean,
): void {
  ctx.effect(() => {
    const disposer = ctx.on('session/event', (session: Session, event: SessionEvent) => {
      const sessionId = String(session.id)
      if (!isArchived(sessionId)) return
      archive.append(sessionId, event)
    })
    return () => disposer()
  }, 'dsh-http-server: session/event archive')
}
