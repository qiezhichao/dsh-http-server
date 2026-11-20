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
import type { ServerResponse } from 'node:http';
/** SSE 流句柄：send 写事件帧、ping 写注释帧、close 结束响应（幂等）。 */
export interface SseStream {
    send(event: string, data: unknown): void;
    /** SSE 注释帧：防中间层（Nginx/网关/浏览器空闲超时）在长沉默期静默掐流。 */
    ping(): void;
    close(): void;
}
/** 开启 SSE 流式通道（立即写 200 头，帧经 send/ping 持续写入直到 close）。 */
export declare function initSseStream(res: ServerResponse): SseStream;
/**
 * SSE 心跳：每 intervalMs（缺省 15s）写一个 `: ping` 注释帧，unref 定时器
 * （不阻进程退出）；返回停止函数（cleanup 时必调）。
 */
export declare function startSseHeartbeat(sse: {
    ping(): void;
}, intervalMs?: number): () => void;
