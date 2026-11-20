/**
 * dsh-http-server — 路由分发
 *
 * 单 match 函数分发全部 REST 路由（任务 3 全量接入，任务 4 追加 SSE）。
 * 错误响应契约（裁决逐字）：409 {"error":"session busy"}、404 {"error":"not found"}、
 * 400 {"error":"invalid cwd"}（body 无 prompt 亦 400）。
 */
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { EventArchive } from './events.js';
import { SessionStore } from './sessions.js';
import type { Context } from '@deepseek-ai/cordis';
import type { HttpServerConfig } from './config.js';
/** 插件版本号，与 package.json 保持一致。 */
export declare const PLUGIN_VERSION = "0.1.0";
/** 解析请求路径（pathname + 查询串，query 由 match 内 split('?') 消费；解析失败时原样返回兜底）。 */
export declare function requestPath(req: IncomingMessage): string;
/** 剥离路由前缀，返回前缀后的子路径（含查询串；未命中前缀时原样返回）。 */
export declare function stripPrefix(path: string, prefix: string): string;
/** 统一 JSON 响应：Content-Type 恒为 application/json; charset=utf-8。 */
export declare function sendJson(res: ServerResponse, statusCode: number, payload: unknown): void;
/** 统一错误响应：{"ok":false,"error":...,"code":...}。 */
export declare function sendError(res: ServerResponse, statusCode: number, message: string, code?: string): void;
/** 路由分发（match 形态沿任务 1，注入依赖经 matchContext 参数传入）。 */
export declare function match(req: IncomingMessage, res: ServerResponse, subPath: string, deps: MatchDeps): boolean;
/** match 的注入依赖（index.ts 装配时构造）。 */
export interface MatchDeps {
    ctx: Context;
    config: HttpServerConfig;
    store: SessionStore;
    archive: EventArchive;
}
