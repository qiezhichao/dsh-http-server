/**
 * dsh-http-server — DSH 进程内 API 适配层
 *
 * 本模块是插件内唯一触碰 DSH 内部 API 的地方（create/resume/followup/whenIdle/
 * preset 挂载/缺省模型/模型目录），任务 3 的 SessionStore 只消费这里的签名。
 *
 * 类型对齐 @deepseek-ai/*@0.2.2-rc.5（部署宿主 dsh 0.2.2-rc.5 实际捆绑的版本线）：
 *  - create/resume 的 setup 写法与官方 SDK 一致（master packages/sdk/server/src/server.ts
 *    的 createSession 594-641 行；npm @deepseek-ai/dsh-sdk-jsonrpc-server@0.2.2-rc.5
 *    lib/index.js 同款实现）：先挂 preset、再 model.select，resume 时同路由保留已记录的
 *    reasoningEffort。
 *  - agentPresets / agentDefaultModel 两个宿主服务不在本包类型依赖里（分别由
 *    @deepseek-ai/dsh-agent-preset-registry 与 @deepseek-ai/dsh-agent-default-model
 *    提供，部署 web 宿主经 dsh-web-app / dsh-base 组合内置），此处按最小消费面做本地
 *    声明并以 ctx.get() 探测取用——与 dsh-webapi src/sessions.ts 73 行（agentPresets）、
 *    534 行（agentDefaultModel.currentSelection）的官方插件用法一致。
 */

import { randomUUID } from 'node:crypto'
import type { Context } from '@deepseek-ai/cordis'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { AgentHandle, AgentOptions, AgentSetup, ModelSelection } from '@deepseek-ai/dsh-agent'
import { createUserMessage, ReasoningEffortId } from '@deepseek-ai/dsh-llm'
import type { SessionId } from '@deepseek-ai/dsh-session'
import type { HttpServerConfig } from './config.js'

/** 宿主 agentPresets 服务的最小消费面（完整 API 见 dsh-agent-preset-registry）。 */
interface AgentPresetsLike {
  /** 把 preset id 声明的工具面挂到 agent 的 scoped context 上；id 缺省用注册表默认。 */
  mount(agentCtx: Context, id?: string): Promise<unknown>
}

/** 宿主 agentDefaultModel 服务的最小消费面（完整 API 见 dsh-agent-default-model）。 */
interface AgentDefaultModelLike {
  /** 读 web UI 配置的部署缺省模型选择（同步读 volatile 配置）。 */
  currentSelection(): { provider: string; model: string; reasoningEffort?: ReasoningEffortId }
}

declare module '@deepseek-ai/cordis' {
  interface Context {
    /** preset 注册表：部署 web 宿主内置，最小 SDK 组合可能缺失，取用前判空。 */
    agentPresets?: AgentPresetsLike
    /** 缺省模型服务：dsh-base 引入，缺省模型跟随 web UI 时读取，取用前判空。 */
    agentDefaultModel?: AgentDefaultModelLike
  }
}

/** 一次会话的模型路由（provider + model + 可选生成参数）。 */
export interface AgentRoute {
  provider: string
  model: string
  reasoningEffort?: string
  maxTokens?: number
}

/**
 * 解析本次请求的模型路由。逐字段优先级：override > config > 宿主缺省
 * （ctx.agentDefaultModel.currentSelection()，即 web UI 的默认模型）；全部缺省时
 * provider/model 为空串（调用方应视为"未指定"，建会话时省略对应字段）。
 */
export function resolveRoute(ctx: Context, config: HttpServerConfig, override?: { model?: string }): AgentRoute {
  const fallback = ctx.get('agentDefaultModel')?.currentSelection()
  return {
    provider: config.provider || fallback?.provider || '',
    model: override?.model || config.model || fallback?.model || '',
    reasoningEffort: config.reasoningEffort ?? fallback?.reasoningEffort,
    maxTokens: config.maxTokens,
  }
}

/** AgentRoute → dsh-agent 的 AgentOptions（空串字段省略；effort 经属主工厂转为品牌类型）。 */
function toAgentOptions(route: AgentRoute): AgentOptions {
  const options: AgentOptions = {}
  if (route.provider) options.provider = route.provider
  if (route.model) options.model = route.model
  if (route.reasoningEffort) options.reasoningEffort = ReasoningEffortId(route.reasoningEffort)
  if (route.maxTokens !== undefined) options.maxTokens = route.maxTokens
  return options
}

/**
 * create/resume 共用的 setup：先挂 preset 工具面，再把路由写为该 agent 的模型选择。
 * 参照官方 SDK createSession：未显式指定 effort 且已记录选择与目标同 provider/model
 * 时，保留已记录的 effort（resume 语义；create 时无已记录选择，等价于直接 select）。
 * presetId 传 undefined → 用注册表默认 preset；传空串 → 跳过挂载；宿主无
 * agentPresets 服务（最小组合）→ 跳过挂载。
 */
function buildSetup(ctx: Context, route: AgentRoute, presetId: string | undefined): AgentSetup {
  return async (agentCtx, agent) => {
    const presets = ctx.get('agentPresets')
    if (presets !== undefined && presetId !== '') await presets.mount(agentCtx, presetId)
    if (!route.provider || !route.model) return
    const selection: ModelSelection = { provider: route.provider, model: route.model }
    if (route.reasoningEffort) {
      selection.reasoningEffort = ReasoningEffortId(route.reasoningEffort)
    } else {
      const previous = agent.model.current()
      if (previous?.reasoningEffort !== undefined
        && previous.provider === selection.provider && previous.model === selection.model) {
        selection.reasoningEffort = previous.reasoningEffort
      }
    }
    agent.model.select(selection)
  }
}

/**
 * 新建一个 agent 会话（sessionId 为进程内新生成的 UUID）。
 * presetId 非空串时写入 durable 会话元数据 meta.agentPreset（生态回读用，
 * 官方 session-controller 同款）；undefined/'' 时省略——前者跟随注册表默认，
 * 后者不挂载，回读方见缺失即知跟随默认。
 */
export async function createAgent(ctx: Context, route: AgentRoute, cwd: string, presetId?: string): Promise<AgentHandle> {
  return ctx.agents.create({
    sessionId: brandString<SessionId>(randomUUID()),
    meta: { cwd, ...(presetId ? { agentPreset: presetId } : {}) },
    agentOptions: toAgentOptions(route),
    setup: buildSetup(ctx, route, presetId),
  })
}

/**
 * 在已持久化的会话上恢复 agent（路由写法替换恢复出的旧模型选择）。
 * 注意：0.2.2-rc.5 的 ResumeAgentOptions 没有 meta 字段——resume 恢复的是既有
 * 持久化会话，durable 元数据（含 agentPreset）在创建时已固化（见上方 createAgent），
 * 官方 SDK 的 resume 分支同样不写 meta；preset 只经 setup 重新挂载到新 scoped world。
 */
export async function resumeAgent(ctx: Context, sessionId: string, route: AgentRoute, presetId?: string): Promise<AgentHandle> {
  return ctx.agents.resume({
    resumeSessionId: brandString<SessionId>(sessionId),
    agentOptions: toAgentOptions(route),
    setup: buildSetup(ctx, route, presetId),
  })
}

/** 向 agent 提交一条普通用户消息（排队为独立 turn 并唤醒驱动），返回 messageId。 */
export function sendPrompt(handle: AgentHandle, text: string): string {
  const message = createUserMessage({ content: [{ type: 'text', text }], source: { kind: 'user' } })
  handle.agent.followup(message)
  return message.id
}

/** 等待该 agent 的全部活动收敛到静默（turn 结束、维护任务清空）。 */
export function whenIdle(handle: AgentHandle): Promise<void> {
  return handle.agent.whenIdle()
}

/** 释放句柄：停驱动、注销 agent、移除会话、回滚其 scoped world。 */
export async function disposeHandle(handle: AgentHandle): Promise<void> {
  await handle.dispose()
}

/**
 * 模型目录：providers 为注册的 provider id 列表；models 为 {provider, id} 扁平列表
 * （各 provider 的模型 id 空间独立，扁平对带归属）。LLM 目录是 advisory 的，单个
 * provider 枚举失败只跳过该家，不影响整体返回。
 */
export async function listModels(ctx: Context): Promise<{ providers: string[]; models: Array<{ provider: string; id: string }> }> {
  const providers = ctx.llm.listProviders().map((entry) => entry.id)
  const models: Array<{ provider: string; id: string }> = []
  for (const provider of providers) {
    try {
      for (const info of await ctx.llm.listModels(provider)) models.push({ provider: info.provider, id: info.id })
    } catch { /* advisory 目录：单家失败跳过 */ }
  }
  return { providers, models }
}
