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
/** 单会话归档上限，超出丢最旧（规格「我替你做的决定」第 9 条）。 */
const ARCHIVE_LIMIT = 10000;
/** DSH TokenUsage（inputTokens/outputTokens/cacheReadTokens/cacheWriteTokens，可缺省）→ HTTP 视图。 */
function toUsage(data) {
    if (typeof data !== 'object' || data === null)
        return undefined;
    const usage = data.usage;
    if (typeof usage !== 'object' || usage === null)
        return undefined;
    const { inputTokens, outputTokens, cacheReadTokens, cacheWriteTokens } = usage;
    return {
        input: inputTokens ?? 0,
        output: outputTokens ?? 0,
        cacheRead: cacheReadTokens ?? 0,
        cacheWrite: cacheWriteTokens ?? 0,
    };
}
/** assistant/message 的 message.content 文本块拼接（对齐 dsh-llm extractTextContent 语义）。 */
function textOf(message) {
    if (typeof message !== 'object' || message === null)
        return '';
    const content = message.content;
    if (!Array.isArray(content))
        return '';
    let text = '';
    for (const block of content) {
        if (typeof block === 'object' && block !== null
            && block.type === 'text'
            && typeof block.text === 'string') {
            text += block.text;
        }
    }
    return text;
}
/** 运行中判定：归档里存在 turn/start 无配对 turn/end，或句柄 status==='running'。 */
function runningFromEntries(entries, handle) {
    if (handle !== undefined && handle.agent.status === 'running')
        return true;
    let open = false;
    for (const entry of entries) {
        if (entry.view.type === 'turn/start')
            open = true;
        else if (entry.view.type === 'turn/end')
            open = false;
    }
    return open;
}
/** 内存事件归档：sessionId → 有序条目列表（append 顺序即 seq 顺序）。 */
export class EventArchive {
    sessions = new Map();
    /** session/event 订阅回调（index.ts 装配）。归档条目并滚动派生字段。 */
    append(sessionId, event) {
        let entries = this.sessions.get(sessionId);
        if (entries === undefined) {
            entries = [];
            this.sessions.set(sessionId, entries);
        }
        const last = entries[entries.length - 1];
        // 0.2.2-rc.5 真实形状：usage 挂在 assistant/message.data.usage（turn/end 无 usage
        // 字段），滚动保留到条目上；turn/end 若将来携带 usage 也覆盖（双兼容）。
        const usageFromThis = event.type === 'assistant/message' || event.type === 'turn/end'
            ? toUsage(event.data)
            : undefined;
        const entry = {
            view: { type: event.type, seq: event.seq, time: event.time, data: event.data },
            lastUsage: usageFromThis ?? last?.lastUsage,
            lastAssistantText: event.type === 'assistant/message'
                ? textOf(event.data.message)
                : (last?.lastAssistantText ?? ''),
        };
        entries.push(entry);
        // 环形截断丢最旧时，滚动派生字段随 last 条目保留（usage/text 不受丢旧影响）。
        if (entries.length > ARCHIVE_LIMIT)
            entries.shift();
    }
    /** seq > since 的事件视图（丢最旧不影响游标：seq 单调）。无归档的会话返回空。 */
    since(sessionId, since) {
        const entries = this.sessions.get(sessionId);
        if (entries === undefined)
            return [];
        const events = [];
        for (const entry of entries) {
            if (entry.view.seq > since)
                events.push(entry.view);
        }
        return events;
    }
    /** 会话运行状态（turn 边界 + 句柄 status 双源判定）。 */
    statusOf(sessionId, handle) {
        return runningFromEntries(this.sessions.get(sessionId) ?? [], handle) ? 'running' : 'idle';
    }
    /** 最后一条 assistant/message 文本。 */
    lastAssistantTextOf(sessionId) {
        const entries = this.sessions.get(sessionId);
        return entries === undefined || entries.length === 0 ? '' : (entries[entries.length - 1]?.lastAssistantText ?? '');
    }
    /** 最后一个 turn/end 的 usage。 */
    usageOf(sessionId) {
        const entries = this.sessions.get(sessionId);
        return entries === undefined || entries.length === 0 ? undefined : entries[entries.length - 1]?.lastUsage;
    }
    /** 归档里该会话最新的 turn 序号（tasks 路由的 taskId 后缀；无归档为 0）。 */
    lastTurnOf(sessionId) {
        const entries = this.sessions.get(sessionId);
        if (entries === undefined)
            return 0;
        let turn = 0;
        for (const entry of entries) {
            if (entry.view.type === 'turn/start' || entry.view.type === 'turn/end') {
                const value = entry.view.data.turn;
                if (typeof value === 'number')
                    turn = value;
            }
        }
        return turn;
    }
    /** DELETE 会话时清归档（事件流已断，无需保留）。 */
    clear(sessionId) {
        this.sessions.delete(sessionId);
    }
    /** 全部有归档的 sessionId（disposeAll 清理用）。 */
    sessionIds() {
        return [...this.sessions.keys()];
    }
}
/**
 * 装配 `session/event` 订阅：只归档 isArchived 认可的会话（index.ts 传入
 * 「本插件 SessionStore 在册」判定）。宿主其他来源的会话（如 web UI 自建的）
 * 事件直接忽略——否则外全会话永不清理，长期运行无上限驻留（审查 I-3）。
 * 返回 effect 清理函数（ctx.on 返回 disposer，装进 ctx.effect 供卸载时退订）。
 */
export function attachEventArchive(ctx, archive, isArchived) {
    ctx.effect(() => {
        const disposer = ctx.on('session/event', (session, event) => {
            const sessionId = String(session.id);
            if (!isArchived(sessionId))
                return;
            archive.append(sessionId, event);
        });
        return () => disposer();
    }, 'dsh-http-server: session/event archive');
}
//# sourceMappingURL=events.js.map