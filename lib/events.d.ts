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
import type { AgentHandle } from '@deepseek-ai/dsh-agent';
import type { SessionEvent } from '@deepseek-ai/dsh-session';
import type { Context } from '@deepseek-ai/cordis';
import type { SessionEventView, TokenUsage } from './http-types.js';
/** 内存事件归档：sessionId → 有序条目列表（append 顺序即 seq 顺序）。 */
export declare class EventArchive {
    private readonly sessions;
    /** session/event 订阅回调（index.ts 装配）。归档条目并滚动派生字段。 */
    append(sessionId: string, event: SessionEvent): void;
    /** seq > since 的事件视图（丢最旧不影响游标：seq 单调）。无归档的会话返回空。 */
    since(sessionId: string, since: number): SessionEventView[];
    /** 会话运行状态（turn 边界 + 句柄 status 双源判定）。 */
    statusOf(sessionId: string, handle: AgentHandle | undefined): 'idle' | 'running';
    /** 最后一条 assistant/message 文本。 */
    lastAssistantTextOf(sessionId: string): string;
    /** 最后一个 turn/end 的 usage。 */
    usageOf(sessionId: string): TokenUsage | undefined;
    /** 归档里该会话最新的 turn 序号（tasks 路由的 taskId 后缀；无归档为 0）。 */
    lastTurnOf(sessionId: string): number;
    /** DELETE 会话时清归档（事件流已断，无需保留）。 */
    clear(sessionId: string): void;
    /** 全部有归档的 sessionId（disposeAll 清理用）。 */
    sessionIds(): string[];
}
/**
 * 装配 `session/event` 订阅：只归档 isArchived 认可的会话（index.ts 传入
 * 「本插件 SessionStore 在册」判定）。宿主其他来源的会话（如 web UI 自建的）
 * 事件直接忽略——否则外全会话永不清理，长期运行无上限驻留（审查 I-3）。
 * 返回 effect 清理函数（ctx.on 返回 disposer，装进 ctx.effect 供卸载时退订）。
 */
export declare function attachEventArchive(ctx: Context, archive: EventArchive, isArchived: (sessionId: string) => boolean): void;
