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
import type { PlatformPath } from 'node:path';
import type { Context } from '@deepseek-ai/cordis';
import type { AgentHandle } from '@deepseek-ai/dsh-agent';
import type { EventArchive } from './events.js';
import type { HttpServerConfig } from './config.js';
import type { CreateSessionBody } from './http-types.js';
/** 会话不存在（内存无 + 持久化 stat 无 + resume 失败）→ 路由层 404。 */
export declare class NotFoundError extends Error {
    constructor(sessionId: string);
}
/** cwd resolve 后逃出 workspaceRoot → 路由层 400。 */
export declare class CwdEscapeError extends Error {
    constructor(cwd: string, root: string);
}
/**
 * open 状态的 ask_user_question 挂起（stream 生命周期内）：
 * agent 调 ask 工具 → UserQuestionService.ask 经 `user-questions/request`
 * waterfall 分发 → 本插件 answerer 捕获（agent 归属本会话）→ 登记本记录，
 * 返回 pending Promise 等 HTTP /answer 的 resolve({answers})。
 * cleanup（收流/断开）时 reject，防 handle 永挂 running。
 */
export interface PendingAsk {
    /** 本批问题（id 集合校验用：answer 批每题恰一项、id 一一对应）。 */
    questions: Array<{
        id: unknown;
    }>;
    /** waterfall answerer 返回的 pending Promise 的 resolve。 */
    resolve: (answer: {
        answers: unknown;
    }) => void;
    /** 同一 Promise 的 reject（流收口时触发 ASK 收尾）。 */
    reject: (error: unknown) => void;
}
/** 一个在册会话：agent 句柄、resolve 后的 cwd、忙闲门闩。 */
export interface SessionRecord {
    handle: AgentHandle;
    cwd: string;
    busy: boolean;
    /** open 状态挂起的 ask（stream answerer 登记；无则 undefined）。 */
    pendingAsk?: PendingAsk;
    /** 最近一次 question 帧的 tool call id（open 状态 /answer 的 callId 校验锚）。 */
    lastQuestionCallId?: string;
}
/** 会话登记簿：创建/恢复/忙闩/删除，与归档清理解耦。 */
export declare class SessionStore {
    private readonly ctx;
    private readonly config;
    private readonly archive;
    /** path 模块注入点：默认平台实现；win32 用例/未来测试可传 nodePath.win32 模拟。 */
    private readonly path;
    private readonly records;
    constructor(ctx: Context, config: HttpServerConfig, archive: EventArchive, 
    /** path 模块注入点：默认平台实现；win32 用例/未来测试可传 nodePath.win32 模拟。 */
    path?: PlatformPath);
    /**
     * cwd 沙箱校验：相对 workspaceRoot resolve（body.cwd 缺省 = 根本身），
     * resolve 后必须位于根下（含等于），否则 CwdEscapeError。
     * 前缀比较用 path.sep 而非硬编码 '/'（win32 resolve 产物是 '\\' 分隔），
     * resolve 已折叠全部 '..'，前缀命中即必在根下。
     * 返回 resolve 后的绝对路径（create 的 meta.cwd 用它，持久化 header 亦固化它）。
     */
    private validateCwd;
    /**
     * 取在册会话；不在册时的语义（brief 步骤 3 逐字）：
     *  - sessionId === undefined → 新建（进程内新 UUID，createAgent）
     *  - 不存在但可恢复（sessionPersistence.stat 命中）→ resumeAgent
     *  - 不存在且不可恢复（stat 失败/未命中、无持久化服务）→ NotFoundError
     * cwd 校验先行（新建与 resume 前都校验——resume 时 body.cwd 缺省沿用持久化
     * header 的 cwd；显式给出时校验并以其为准）。
     */
    getOrCreate(sessionId: string | undefined, body: CreateSessionBody): Promise<SessionRecord>;
    /** 在册会话（不触发恢复；路由层先 probe 再动门闩用）。 */
    get(sessionId: string): SessionRecord | undefined;
    /** 忙闩查询：不在册视为不忙（409 判定前路由层已确保在册）。 */
    isBusy(sessionId: string): boolean;
    /** 置忙。 */
    setBusy(sessionId: string): void;
    /** 清忙。 */
    clearBusy(sessionId: string): void;
    /** 删除会话：dispose 句柄 + 注销 + 清归档。不存在时静默（DELETE 幂等）。 */
    delete(sessionId: string): Promise<void>;
    /** 插件卸载时清空全部句柄（index.ts 装进 ctx.effect 清理）。 */
    disposeAll(): Promise<void>;
}
