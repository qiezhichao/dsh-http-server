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
import type { Context } from '@deepseek-ai/cordis';
import type { AgentHandle } from '@deepseek-ai/dsh-agent';
import { ReasoningEffortId } from '@deepseek-ai/dsh-llm';
import type { HttpServerConfig } from './config.js';
/** 宿主 agentPresets 服务的最小消费面（完整 API 见 dsh-agent-preset-registry）。 */
interface AgentPresetsLike {
    /** 把 preset id 声明的工具面挂到 agent 的 scoped context 上；id 缺省用注册表默认。 */
    mount(agentCtx: Context, id?: string): Promise<unknown>;
}
/** 宿主 agentDefaultModel 服务的最小消费面（完整 API 见 dsh-agent-default-model）。 */
interface AgentDefaultModelLike {
    /** 读 web UI 配置的部署缺省模型选择（同步读 volatile 配置）。 */
    currentSelection(): {
        provider: string;
        model: string;
        reasoningEffort?: ReasoningEffortId;
    };
}
declare module '@deepseek-ai/cordis' {
    interface Context {
        /** preset 注册表：部署 web 宿主内置，最小 SDK 组合可能缺失，取用前判空。 */
        agentPresets?: AgentPresetsLike;
        /** 缺省模型服务：dsh-base 引入，缺省模型跟随 web UI 时读取，取用前判空。 */
        agentDefaultModel?: AgentDefaultModelLike;
    }
}
/** 一次会话的模型路由（provider + model + 可选生成参数）。 */
export interface AgentRoute {
    provider: string;
    model: string;
    reasoningEffort?: string;
    maxTokens?: number;
}
/**
 * 解析本次请求的模型路由。逐字段优先级：override > config > 宿主缺省
 * （ctx.agentDefaultModel.currentSelection()，即 web UI 的默认模型）；全部缺省时
 * provider/model 为空串（调用方应视为"未指定"，建会话时省略对应字段）。
 */
export declare function resolveRoute(ctx: Context, config: HttpServerConfig, override?: {
    model?: string;
}): AgentRoute;
/**
 * 新建一个 agent 会话（sessionId 为进程内新生成的 UUID）。
 * presetId 非空串时写入 durable 会话元数据 meta.agentPreset（生态回读用，
 * 官方 session-controller 同款）；undefined/'' 时省略——前者跟随注册表默认，
 * 后者不挂载，回读方见缺失即知跟随默认。
 */
export declare function createAgent(ctx: Context, route: AgentRoute, cwd: string, presetId?: string): Promise<AgentHandle>;
/**
 * 在已持久化的会话上恢复 agent（路由写法替换恢复出的旧模型选择）。
 * 注意：0.2.2-rc.5 的 ResumeAgentOptions 没有 meta 字段——resume 恢复的是既有
 * 持久化会话，durable 元数据（含 agentPreset）在创建时已固化（见上方 createAgent），
 * 官方 SDK 的 resume 分支同样不写 meta；preset 只经 setup 重新挂载到新 scoped world。
 */
export declare function resumeAgent(ctx: Context, sessionId: string, route: AgentRoute, presetId?: string): Promise<AgentHandle>;
/** 向 agent 提交一条普通用户消息（排队为独立 turn 并唤醒驱动），返回 messageId。 */
export declare function sendPrompt(handle: AgentHandle, text: string): string;
/** 等待该 agent 的全部活动收敛到静默（turn 结束、维护任务清空）。 */
export declare function whenIdle(handle: AgentHandle): Promise<void>;
/** 释放句柄：停驱动、注销 agent、移除会话、回滚其 scoped world。 */
export declare function disposeHandle(handle: AgentHandle): Promise<void>;
/**
 * 模型目录：providers 为注册的 provider id 列表；models 为 {provider, id} 扁平列表
 * （各 provider 的模型 id 空间独立，扁平对带归属）。LLM 目录是 advisory 的，单个
 * provider 枚举失败只跳过该家，不影响整体返回。
 */
export declare function listModels(ctx: Context): Promise<{
    providers: string[];
    models: Array<{
        provider: string;
        id: string;
    }>;
}>;
export {};
