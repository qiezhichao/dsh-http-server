/**
 * dsh-http-server — 插件入口
 *
 * 把 DSH Agent 能力通过 HTTP 暴露给 Java 等外部系统调用。
 * 任务 1：骨架 + Config + 宿主 webServer 挂载（GET /health）；
 * 任务 3：SessionStore + 事件归档 + 全部 REST 路由装配；
 * 任务 4：SSE 流式端点；任务 5：standalone 独立服务两栖挂载。
 *
 * 两栖装配：路由挂宿主 webServer 前缀（与 web UI 同端口）；
 * config.standalonePort > 0 时另起独立 node:http 服务——两者共用同一个
 * routeHandler（同一份路由/会话/归档状态），用于在同一 profile 内需要
 * 独立端口对外（如仅暴露给 Java 系统的最小面）的部署。
 * 注意：webServer 为必需注入——无 webServer 服务的 profile（如 sdk/headless）
 * 下本插件不会激活；纯 API 部署请使用 web profile（dsh web --no-open）。
 */
import type { Context } from '@deepseek-ai/cordis';
import { type HttpServerConfig, Config } from './config.js';
export { Config };
export declare const name = "dsh-http-server";
/**
 * agents/workingDirectory/llm/webServer 为必需服务。
 * 注意：@deepseek-ai/cordis 4.1 的 Inject.resolve 不剥离 '?' 后缀——数组里的
 * 'webServer?' 会被当作字面服务名等待，导致插件永远 pending（Windows 实测）。
 * 因此全部用非可选注入；cordis 会在服务就绪后自动激活（_refresh 机制）。
 * agentDefaultModel 不入注入表：web profile 未必稳定提供，apply 内经
 * ctx.get('agentDefaultModel') 动态探测（缺省回退 config.provider/model）。
 */
export declare const inject: string[];
export declare function apply(ctx: Context, config: HttpServerConfig): void;
