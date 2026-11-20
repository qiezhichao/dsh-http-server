/**
 * dsh-http-server — 路由分发
 *
 * 单 match 函数分发全部 REST 路由（任务 3 全量接入，任务 4 追加 SSE）。
 * 错误响应契约（裁决逐字）：409 {"error":"session busy"}、404 {"error":"not found"}、
 * 400 {"error":"invalid cwd"}（body 无 prompt 亦 400）。
 */
import { CwdEscapeError, NotFoundError } from './sessions.js';
import { whenIdle, sendPrompt, listModels } from './agent-adapter.js';
import { initSseStream, startSseHeartbeat } from './sse.js';
/** 插件版本号，与 package.json 保持一致。 */
export const PLUGIN_VERSION = '0.1.0';
/** prompt 默认超时毫秒数。 */
const DEFAULT_PROMPT_TIMEOUT_MS = 120000;
/** wait 默认超时毫秒数。 */
const DEFAULT_WAIT_TIMEOUT_MS = 300000;
/** 无效请求体统一 400 响应体。 */
function badRequest(res, message) {
    sendJson(res, 400, { error: message });
}
/** 解析请求路径（pathname + 查询串，query 由 match 内 split('?') 消费；解析失败时原样返回兜底）。 */
export function requestPath(req) {
    const url = req.url ?? '/';
    try {
        const parsed = new URL(url, 'http://x');
        return `${parsed.pathname}${parsed.search}`;
    }
    catch {
        return url;
    }
}
/** 剥离路由前缀，返回前缀后的子路径（含查询串；未命中前缀时原样返回）。 */
export function stripPrefix(path, prefix) {
    if (prefix && path.startsWith(prefix)) {
        const rest = path.slice(prefix.length);
        return rest.startsWith('/') ? rest : `/${rest}`;
    }
    return path;
}
/** 统一 JSON 响应：Content-Type 恒为 application/json; charset=utf-8。 */
export function sendJson(res, statusCode, payload) {
    if (res.headersSent)
        return;
    const data = JSON.stringify(payload);
    res.statusCode = statusCode;
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Content-Length', Buffer.byteLength(data));
    res.end(data);
}
/** 统一错误响应：{"ok":false,"error":...,"code":...}。 */
export function sendError(res, statusCode, message, code) {
    sendJson(res, statusCode, { ok: false, error: message, ...(code === undefined ? {} : { code }) });
}
/** 读取并解析 JSON body（解析失败返回 undefined；上限 1MiB 防滥用）。 */
async function readJsonBody(req) {
    const chunks = [];
    let total = 0;
    for await (const chunk of req) {
        total += chunk.length;
        if (total > 1024 * 1024)
            throw new Error('payload too large');
        chunks.push(chunk);
    }
    const text = Buffer.concat(chunks).toString('utf8');
    if (text.trim() === '')
        return {};
    return JSON.parse(text);
}
/** sleep 毫秒工具（超时竞态用）。 */
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
/** 从已注册路径参数的会话出归档结果（prompt/wait 共用）。 */
function chatResultOf(sessionId, archive, handle, status) {
    return {
        status,
        text: archive.lastAssistantTextOf(sessionId),
        ...(archive.usageOf(sessionId) === undefined ? {} : { usage: archive.usageOf(sessionId) }),
    };
}
/** 从 URL 查询串取 since 参数（非法/缺省为 0）。 */
function sinceFrom(search) {
    const since = search === undefined ? undefined : new URLSearchParams(search).get('since');
    const value = since === null || since === undefined ? 0 : Number(since);
    return Number.isFinite(value) && value >= 0 ? value : 0;
}
/** 安全读取事件 data 的一层字段（非对象/缺键 → undefined）。 */
function fieldOf(data, key) {
    return typeof data === 'object' && data !== null ? data[key] : undefined;
}
/** 事件的 turn 序号（data.turn 非数值 → undefined，即不参与归属判定）。 */
function turnOf(event) {
    const value = fieldOf(event.data, 'turn');
    return typeof value === 'number' ? value : undefined;
}
/**
 * 从 assistant 消息内容块提取正文与思考文本（兼容 text / reasoning / thinking 块，
 * 照搬 dsh-webapi extractAssistantText 语义）。
 */
function extractAssistantText(message) {
    let text = '';
    let reasoning = '';
    const content = fieldOf(message, 'content');
    if (!Array.isArray(content))
        return { text, reasoning };
    for (const block of content) {
        if (typeof block !== 'object' || block === null)
            continue;
        const record = block;
        if (record.type === 'text' && typeof record.text === 'string')
            text += record.text;
        else if ((record.type === 'reasoning' || record.type === 'thinking') && typeof record.text === 'string')
            reasoning += record.text;
    }
    return { text, reasoning };
}
/** 取 full 相对 streamed 的增量：已对齐只补差量；无法对齐且尚未推送过则补全量。 */
function takeSuffix(full, streamed) {
    const f = typeof full === 'string' ? full : '';
    const s = typeof streamed === 'string' ? streamed : '';
    if (f === '')
        return '';
    if (s === '')
        return f;
    if (f === s)
        return '';
    if (f.startsWith(s))
        return f.slice(s.length);
    if (s.startsWith(f))
        return '';
    return f;
}
/**
 * SSE 流式处理（在 match 校验完 body/在册/忙闩并 setBusy 之后进入；门闩持有
 * 到流关闭——done/客户端断开/看门狗收流三路径的 cleanup 均清忙）。
 *
 * 帧映射（规格 §HTTP API 契约）：assistant/chunk → delta/reasoning；
 * tool/call → tool；turn/end → turn/end（usage 沿用归档同源：
 * assistant/message.data.usage 滚动投影，即 REST usage 同款）+ done("[DONE]")；
 * 会话级 error 事件 → error 帧（真实错误信息）+ done 即时收流（照 dsh-webapi
 * 范例，error 是流终结事件）。
 * harness 不发增量 chunk 只给 assistant/message 全文时，用 streamedText/
 * streamedReasoning 差量补推（照搬 dsh-webapi 注释钉死的做法）。
 */
function handleStream(res, sessionId, prompt, stallTimeoutMs, record, deps) {
    const { ctx, store, archive } = deps;
    const sse = initSseStream(res);
    sse.send('connected', { sessionId });
    let cleanedUp = false;
    // 本 step 已推送的文本/思考量：harness 可能不发增量 chunk，只在 assistant/message
    // 给全文，用它做差量补推，既不丢内容也不重复。
    let streamedText = '';
    let streamedReasoning = '';
    // SSE 心跳：每 15s 注释帧，防中间层（Nginx/网关/浏览器空闲超时）在长沉默期静默掐流
    const stopHeartbeat = startSseHeartbeat(sse);
    // 停滞看门狗（与 dsh-webapi 同语义）：不按「整轮总时长」掐断 —— 持续产出
    // （哪怕远超 5 分钟的长文本/多轮工具）永远不打断；只有持续 stallTimeoutMs
    // 收不到任何会话事件（真停滞）才显式报错收流。
    let watchdog;
    const resetStallTimer = () => {
        if (watchdog !== undefined)
            clearTimeout(watchdog);
        watchdog = setTimeout(() => {
            if (cleanedUp)
                return;
            sse.send('error', { message: `turn stalled: no session events for ${Math.round(stallTimeoutMs / 1000)}s` });
            sse.send('done', '[DONE]');
            cleanup();
        }, stallTimeoutMs);
        watchdog.unref();
    };
    resetStallTimer();
    /**
     * 本次流所属的 turn。事件订阅是**会话级**的：若订阅时该会话上一轮尚未收尾
     * （prompt 被排队 / 流开启前已在跑的回合），旧轮次的 turn/end 会被误当作本轮
     * 结束——流提前关闭，调用方拿到半截内容却以为完成了。因此：
     *   - 自己的 turn/start 前到达的 turn/end 一定不属于本次提交，直接忽略；
     *   - 一旦认领了自己的 turn，后续其它 turn 的事件（含 turn/end）一律不转发。
     */
    let ownTurn;
    let sawTurnStart = false;
    const isForeignTurn = (event) => {
        if (!sawTurnStart || ownTurn === undefined)
            return false;
        const turn = turnOf(event);
        return turn !== undefined && turn !== ownTurn;
    };
    // 独立于归档的自持订阅（照 dsh-webapi 模式过滤 session.id，不复用归档通道）
    const unsubscribe = ctx.on('session/event', (session, event) => {
        if (String(session.id) !== sessionId)
            return;
        const view = event;
        if (isForeignTurn(view))
            return;
        try {
            // 任何属于本会话的事件到达都证明流转活着：重置停滞看门狗
            resetStallTimer();
            if (view.type === 'turn/start') {
                if (!sawTurnStart) {
                    sawTurnStart = true;
                    const turn = turnOf(view);
                    if (turn !== undefined)
                        ownTurn = turn;
                }
                return;
            }
            if (view.type === 'assistant/chunk') {
                const chunk = fieldOf(view.data, 'chunk');
                const chunkType = fieldOf(chunk, 'type');
                if (chunkType === 'text-delta') {
                    const raw = fieldOf(chunk, 'text');
                    const text = typeof raw === 'string' ? raw : '';
                    streamedText += text;
                    if (text !== '')
                        sse.send('delta', { delta: text, seq: view.seq });
                }
                else if (chunkType === 'reasoning-delta') {
                    const raw = fieldOf(chunk, 'text');
                    const text = typeof raw === 'string' ? raw : '';
                    streamedReasoning += text;
                    if (text !== '')
                        sse.send('reasoning', { delta: text, seq: view.seq });
                }
                return;
            }
            if (view.type === 'step/start') {
                // 每个 step 是一次独立模型调用：重置差量游标
                streamedText = '';
                streamedReasoning = '';
                return;
            }
            if (view.type === 'assistant/message') {
                // harness 可能不发增量 chunk：从完整 assistant 消息补推差量文本
                const full = extractAssistantText(fieldOf(view.data, 'message'));
                const textSuffix = takeSuffix(full.text, streamedText);
                if (textSuffix !== '') {
                    sse.send('delta', { delta: textSuffix, seq: view.seq });
                    streamedText = full.text;
                }
                const reasonSuffix = takeSuffix(full.reasoning, streamedReasoning);
                if (reasonSuffix !== '') {
                    sse.send('reasoning', { delta: reasonSuffix, seq: view.seq });
                    streamedReasoning = full.reasoning;
                }
                return;
            }
            if (view.type === 'tool/call') {
                // DSH 核心 tool/call 载荷: { turn, step, callId, name, arguments }（arguments 为模型原始 JSON 串）
                const name = fieldOf(view.data, 'name');
                const args = fieldOf(view.data, 'arguments');
                sse.send('tool', {
                    name: typeof name === 'string' ? name : '',
                    arguments: typeof args === 'string' ? args : '',
                    seq: view.seq,
                });
                return;
            }
            if (view.type === 'turn/end') {
                // 尚未见到本轮 turn/start 就来的 turn/end：属于订阅前就已开始的旧轮次，
                // 不是本次提交的结束。
                if (!sawTurnStart)
                    return;
                const reason = fieldOf(view.data, 'reason');
                const usage = archive.usageOf(sessionId);
                sse.send('turn/end', {
                    reason: reason === undefined ? 'completed' : reason,
                    ...(usage === undefined ? {} : { usage }),
                });
                sse.send('done', '[DONE]');
                cleanup();
                return;
            }
            if (view.type === 'error') {
                // 会话级 error 事件（照 dsh-webapi 范例）：error 是流终结事件——必须补
                // done 并关闭，否则消费方（等 done/关流才结束）会永远挂在打开的 SSE 上，
                // 真实错误信息丢失且被看门狗延迟 stallTimeoutMs 后误报为 "turn stalled"。
                // 错误后的恢复交给调用方的对账通道（REST events 轮询），本流不再服务后续事件。
                const message = fieldOf(view.data, 'message');
                sse.send('error', { message: typeof message === 'string' && message !== '' ? message : 'Execution error' });
                sse.send('done', '[DONE]');
                cleanup();
                return;
            }
        }
        catch (error) {
            // 不能静默吞掉：任何 handler 内部错误都要让调用方可见，否则表现为
            // 「connected 之后流无声消失」且无从排查。
            try {
                sse.send('error', { message: `stream handler error: ${String(error)}` });
            }
            catch {
                /* 响应已结束 */
            }
            sse.send('done', '[DONE]');
            cleanup();
        }
    });
    function cleanup() {
        if (cleanedUp)
            return;
        cleanedUp = true;
        try {
            unsubscribe();
        }
        catch {
            /* 退订尽力 */
        }
        try {
            stopHeartbeat();
        }
        catch {
            /* 停心跳尽力 */
        }
        if (watchdog !== undefined)
            clearTimeout(watchdog);
        store.clearBusy(sessionId);
        sse.close();
    }
    // 客户端断开：与 done/看门狗同为收流路径，cleanup 幂等。
    // 断开信号用 res 'close' 而非 dsh-webapi 原样的 req 'close'：Node ≥16 把
    // IncomingMessage 'close' 语义改为「请求消息完成时」——本路由先 readJsonBody
    // 消费了 body，req 'close' 在监听器挂上之前就已发完且客户端断开时不再触发
    // （实测 node http：abort 只触发 res close）；ServerResponse 'close' 在
    // 客户端断开与正常 res.end() 后都触发，后者被 cleanedUp 幂等吸收。
    res.on('close', () => {
        cleanup();
    });
    // 提交 prompt（handleStream 前置校验已置忙；sendPrompt 抛错时任务未启动，
    // error+done 收流并清忙，否则该会话此后所有请求永久 409）
    try {
        sendPrompt(record.handle, prompt);
    }
    catch (error) {
        sse.send('error', { message: `Failed to admit prompt: ${String(error)}` });
        sse.send('done', '[DONE]');
        cleanup();
    }
}
/** 路由分发（match 形态沿任务 1，注入依赖经 matchContext 参数传入）。 */
export function match(req, res, subPath, deps) {
    void (async () => {
        const { ctx, config, store, archive } = deps;
        const method = req.method ?? 'GET';
        const [pathname, search] = subPath.split('?');
        // GET /health → 200（含 providers/models 目录）
        if (method === 'GET' && pathname === '/health') {
            try {
                const catalog = await listModels(ctx);
                sendJson(res, 200, {
                    ok: true,
                    service: 'dsh-http-server',
                    version: PLUGIN_VERSION,
                    providers: catalog.providers,
                    models: catalog.models,
                });
            }
            catch (error) {
                sendError(res, 500, `model catalog failed: ${String(error)}`);
            }
            return;
        }
        // POST /sessions → 201 {sessionId, cwd}
        if (method === 'POST' && pathname === '/sessions') {
            let body;
            try {
                body = (await readJsonBody(req));
            }
            catch {
                badRequest(res, 'invalid body');
                return;
            }
            try {
                const record = await store.getOrCreate(undefined, body);
                const sessionId = String(record.handle.agent.session.id);
                sendJson(res, 201, { sessionId, cwd: record.cwd });
            }
            catch (error) {
                if (error instanceof CwdEscapeError) {
                    sendJson(res, 400, { error: 'invalid cwd' });
                    return;
                }
                sendError(res, 500, String(error));
            }
            return;
        }
        // /sessions/:id/* 路由组：取 id，逐个分发
        const sessionMatch = /^\/sessions\/([^/]+)(\/.*)?$/.exec(pathname);
        if (sessionMatch === null) {
            sendError(res, 404, `Endpoint not found: ${method} ${pathname}`, 'NOT_FOUND');
            return;
        }
        const sessionId = decodeURIComponent(sessionMatch[1] ?? '');
        const action = sessionMatch[2] ?? '';
        if (action === '/prompt' && method === 'POST') {
            let body;
            try {
                body = (await readJsonBody(req));
            }
            catch {
                badRequest(res, 'invalid body');
                return;
            }
            if (typeof body.prompt !== 'string' || body.prompt === '') {
                badRequest(res, 'invalid prompt');
                return;
            }
            // 先确保在册（不在册则尝试恢复），恢复不了 404。
            let record;
            try {
                record = await store.getOrCreate(sessionId, {});
            }
            catch (error) {
                if (error instanceof NotFoundError) {
                    sendJson(res, 404, { error: 'not found' });
                    return;
                }
                sendError(res, 500, String(error));
                return;
            }
            if (store.isBusy(sessionId)) {
                sendJson(res, 409, { error: 'session busy' });
                return;
            }
            store.setBusy(sessionId);
            try {
                sendPrompt(record.handle, body.prompt);
                const timeoutMs = body.timeoutMs ?? DEFAULT_PROMPT_TIMEOUT_MS;
                const idle = whenIdle(record.handle).then(() => 'idle');
                const timer = sleep(timeoutMs).then(() => 'timeout');
                const winner = await Promise.race([idle, timer]);
                const result = chatResultOf(sessionId, archive, record.handle, winner === 'idle' ? 'completed' : 'timeout');
                sendJson(res, 200, result);
            }
            catch (error) {
                const result = chatResultOf(sessionId, archive, record.handle, 'error');
                sendJson(res, 200, { ...result, ...(result.text === '' ? { text: String(error) } : {}) });
            }
            finally {
                store.clearBusy(sessionId);
            }
            return;
        }
        if (action === '/tasks' && method === 'POST') {
            let body;
            try {
                body = (await readJsonBody(req));
            }
            catch {
                badRequest(res, 'invalid body');
                return;
            }
            if (typeof body.prompt !== 'string' || body.prompt === '') {
                badRequest(res, 'invalid prompt');
                return;
            }
            let record;
            try {
                record = await store.getOrCreate(sessionId, {});
            }
            catch (error) {
                if (error instanceof NotFoundError) {
                    sendJson(res, 404, { error: 'not found' });
                    return;
                }
                sendError(res, 500, String(error));
                return;
            }
            if (store.isBusy(sessionId)) {
                sendJson(res, 409, { error: 'session busy' });
                return;
            }
            store.setBusy(sessionId);
            // turn 序号取当前归档里该 session 的 turn（sendPrompt 会触发 turn/start，
            // 但事件异步到达，taskId 在这里取「即将开启的轮次」用 whenIdle 完成后清忙；
            // 202 语义下调用方轮询 events 以 turn/end 判完成）。
            try {
                const turn = archive.lastTurnOf(sessionId) + 1;
                sendPrompt(record.handle, body.prompt);
                sendJson(res, 202, { taskId: `${sessionId}:${turn}` });
            }
            catch (error) {
                // sendPrompt 抛错时任务未启动，必须立即清忙——否则该会话此后所有
                // prompt/tasks 永久 409（prompt 路由的 finally 同理）。
                store.clearBusy(sessionId);
                sendError(res, 500, String(error));
                return;
            }
            // 异步守护：任务跑完清忙（不 await，路由立即返回）。
            void whenIdle(record.handle)
                .catch(() => undefined)
                .then(() => store.clearBusy(sessionId));
            return;
        }
        if (action === '/events' && method === 'GET') {
            try {
                await store.getOrCreate(sessionId, {});
            }
            catch (error) {
                if (error instanceof NotFoundError) {
                    sendJson(res, 404, { error: 'not found' });
                    return;
                }
                sendError(res, 500, String(error));
                return;
            }
            const since = sinceFrom(search);
            const events = archive.since(sessionId, since);
            const result = {
                events,
                nextSince: events.length === 0 ? since : events[events.length - 1].seq,
                status: archive.statusOf(sessionId, store.get(sessionId)?.handle),
                lastAssistantText: archive.lastAssistantTextOf(sessionId),
            };
            sendJson(res, 200, result);
            return;
        }
        if (action === '/wait' && method === 'POST') {
            let body;
            try {
                body = (await readJsonBody(req));
            }
            catch {
                body = {};
            }
            let record;
            try {
                record = await store.getOrCreate(sessionId, {});
            }
            catch (error) {
                if (error instanceof NotFoundError) {
                    sendJson(res, 404, { error: 'not found' });
                    return;
                }
                sendError(res, 500, String(error));
                return;
            }
            const timeoutMs = body.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS;
            const idle = whenIdle(record.handle).then(() => 'idle');
            const timer = sleep(timeoutMs).then(() => 'timeout');
            const winner = await Promise.race([idle, timer]);
            const result = chatResultOf(sessionId, archive, record.handle, winner === 'idle' ? 'completed' : 'timeout');
            sendJson(res, 200, result);
            return;
        }
        if (action === '/stream' && method === 'POST') {
            let body;
            try {
                body = (await readJsonBody(req));
            }
            catch {
                badRequest(res, 'invalid body');
                return;
            }
            if (typeof body.prompt !== 'string' || body.prompt === '') {
                badRequest(res, 'invalid prompt');
                return;
            }
            let record;
            try {
                record = await store.getOrCreate(sessionId, {});
            }
            catch (error) {
                if (error instanceof NotFoundError) {
                    sendJson(res, 404, { error: 'not found' });
                    return;
                }
                sendError(res, 500, String(error));
                return;
            }
            if (store.isBusy(sessionId)) {
                sendJson(res, 409, { error: 'session busy' });
                return;
            }
            // 202 不适用于 stream：长连接即「在跑」。门闩持有到流关闭
            // （done/客户端断开/看门狗收流），由 handleStream 的 cleanup 统一清忙。
            store.setBusy(sessionId);
            const stallTimeoutMs = typeof body.stallTimeoutMs === 'number' && body.stallTimeoutMs > 0
                ? body.stallTimeoutMs
                : config.stallTimeoutMs;
            handleStream(res, sessionId, body.prompt, stallTimeoutMs, record, deps);
            return;
        }
        if (action === '' && method === 'DELETE') {
            await store.delete(sessionId);
            res.statusCode = 204;
            res.end();
            return;
        }
        sendError(res, 404, `Endpoint not found: ${method} ${pathname}`, 'NOT_FOUND');
    })().catch((error) => {
        sendError(res, 500, String(error));
    });
    return true;
}
//# sourceMappingURL=router.js.map