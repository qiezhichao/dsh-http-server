/**
 * dsh-http-server — REST 请求/响应 DTO
 *
 * 与规格契约（docs/superpowers/specs §HTTP API 契约）一一对应的纯类型文件。
 * prompt 默认 timeoutMs=120000、wait 默认 300000 的缺省值在 router 层取用。
 */

/** POST /sessions 请求体。cwd 相对 workspaceRoot 或绝对路径；model 覆盖插件配置。 */
export interface CreateSessionBody {
  cwd?: string
  model?: string
}

/** POST /sessions/:id/prompt 请求体。timeoutMs 缺省 120000。 */
export interface PromptBody {
  prompt: string
  timeoutMs?: number
}

/** POST /sessions/:id/tasks 请求体。callbackUrl 本期预留不实现。 */
export interface TaskBody {
  prompt: string
  callbackUrl?: string
}

/** POST /sessions/:id/wait 请求体。timeoutMs 缺省 300000。 */
export interface WaitBody {
  timeoutMs?: number
}

/** POST /sessions/:id/stream 请求体。stallTimeoutMs 缺省取插件配置 stallTimeoutMs。 */
export interface StreamBody {
  prompt: string
  /** 停滞看门狗毫秒数：持续该时长无任何会话事件才收流（非整轮时长上限）。 */
  stallTimeoutMs?: number
}

/** token 用量视图（DSH TokenUsage 的 HTTP 投影，字段名为适配 Java 的短名）。 */
export interface TokenUsage {
  input: number
  output: number
  cacheRead: number
  cacheWrite: number
}

/** prompt/wait 的同步响应。 */
export interface ChatResult {
  status: 'completed' | 'timeout' | 'error'
  text: string
  usage?: TokenUsage
}

/** 归档事件视图：SessionEvent 的 { type, seq, time, data } 原样投影。 */
export interface SessionEventView {
  type: string
  seq: number
  time: number
  data: unknown
}

/** GET /sessions/:id/events 响应。 */
export interface EventsResult {
  events: SessionEventView[]
  nextSince: number
  status: 'idle' | 'running'
  lastAssistantText: string
}
