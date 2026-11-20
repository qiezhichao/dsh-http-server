/**
 * dsh-http-server — SSE 流基础设施
 *
 * initSseStream 把 ServerResponse 变成 text/event-stream 通道（send 事件帧 /
 * ping 注释帧 / close 收流），startSseHeartbeat 每 15s 写 `: ping` 注释帧防空闲
 * 掐流。两者均为任务 4 照搬 dsh-webapi src/streaming.ts 的同名实现模式。
 *
 * 帧格式钉死为全量 JSON：`event: <e>\ndata: <JSON>\n\n` —— done 帧即
 * data: "[DONE]"（规格契约 §HTTP API 契约 turn/end/done 帧行）。
 * 标准 SSE 解析器忽略 `: ...` 注释行，心跳不污染消费方的事件流。
 */
/** 开启 SSE 流式通道（立即写 200 头，帧经 send/ping 持续写入直到 close）。 */
export function initSseStream(res) {
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
    });
    return {
        send(event, data) {
            if (res.writableEnded)
                return;
            res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
        },
        ping() {
            if (res.writableEnded)
                return;
            res.write(': ping\n\n');
        },
        close() {
            if (!res.writableEnded)
                res.end();
        },
    };
}
/**
 * SSE 心跳：每 intervalMs（缺省 15s）写一个 `: ping` 注释帧，unref 定时器
 * （不阻进程退出）；返回停止函数（cleanup 时必调）。
 */
export function startSseHeartbeat(sse, intervalMs = 15000) {
    const timer = setInterval(() => {
        try {
            sse.ping();
        }
        catch {
            /* 响应已结束：最终由 cleanup 收尾 */
        }
    }, intervalMs);
    timer.unref();
    return () => clearInterval(timer);
}
//# sourceMappingURL=sse.js.map