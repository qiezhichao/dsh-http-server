/**
 * dsh-http-server — 插件配置 schema 与同构类型
 */
import z from '@deepseek-ai/schemastery';
/** 插件配置的同构类型（与下方 Config schema 一一对应）。 */
export interface HttpServerConfig {
    /** API 路由前缀，挂宿主 webServer 与 standalone 服务共用。 */
    pathPrefix: string;
    /** standalone 独立服务监听地址（standalonePort > 0 时生效）。 */
    standaloneHost: string;
    /** standalone 端口；0 = 禁用（本期仅占位，任务 5 启用）。 */
    standalonePort: number;
    /** 创建会话时挂载的 per-agent 工具面 preset id。 */
    agentPreset: string;
    /** 缺省 provider；缺省跟随 web UI 默认模型（agentDefaultModel）。 */
    provider?: string;
    /** 缺省 model；缺省跟随 web UI 默认模型。 */
    model?: string;
    /** 缺省推理力度。 */
    reasoningEffort?: string;
    /** 缺省最大生成 token 数。 */
    maxTokens?: number;
    /** 会话工作目录根：请求 cwd resolve 后必须位于其下。 */
    workspaceRoot: string;
    /** SSE 停滞看门狗毫秒数（无任何会话事件持续该时长才收流）。 */
    stallTimeoutMs: number;
}
export declare const Config: z<HttpServerConfig>;
