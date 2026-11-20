# dsh-http-server

把 DSH（DeepSeek Harness）的 Agent 能力通过 HTTP 暴露给 Java 等外部系统调用的
DSH 插件（bundle）。在一个 DSH 宿主进程内提供会话全生命周期 HTTP API：
建会话、同步/异步 prompt、事件轮询、等待、SSE 流式、删除，以及跨重启的
会话自动恢复（sessionPersistence.stat 探测 → resume）。

## 端点一览（前缀默认 `/api/v1`）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 + 模型目录 `{ok, service, version, providers, models}` |
| POST | `/sessions` | 建会话 → `201 {sessionId, cwd}`（body 可选 `cwd`/`model`，受 `workspaceRoot` 沙箱约束） |
| POST | `/sessions/:id/prompt` | 同步提问 → `200 {status, text, error?, usage?}`（`status`: completed/timeout/error；turn 以 error 收尾时 `error` 带原因，如模型路由无 key） |
| POST | `/sessions/:id/tasks` | 异步任务 → `202 {taskId}`（完成与否经 events 轮询 turn/end 判定） |
| GET | `/sessions/:id/events?since=N` | 事件增量轮询 → `{events, nextSince, status, lastAssistantText}` |
| POST | `/sessions/:id/wait` | 等待在跑任务收敛 → `200 {status, text, error?, usage?}` |
| POST | `/sessions/:id/stream` | SSE 流式（connected/delta/reasoning/tool/**tool_result**/**question**/turn/end/done 帧；心跳 15s；停滞看门狗） |
| POST | `/sessions/:id/answer` | 回答流中挂起的 `ask_user_question` → `200 {accepted:true}`（不占忙闩；`404 question not found` / `400` 坏批 / `409 reply queued`） |
| DELETE | `/sessions/:id` | 处置会话 → `204`（幂等；内存处置语义，见下「跨重启恢复」） |

错误契约：`409 {"error":"session busy"}`、`404 {"error":"not found"}`、
`400 {"error":"invalid cwd"}` / `400 {"error":"invalid prompt"}`。

### ask_user_question 交互（answer 端点 + question 帧）

模型调 `ask_user_question` 工具时 turn 挂起等待人类回答（timed 工具）。SSE 流发
**`question` 帧** `{callId, questions, seq}`——问题/选项已结构化外发（不必挖
tool 帧的 arguments JSON）；消费方渲染选项，用户选择后调
`POST /sessions/:id/answer` 回传：

```json
{ "callId": "ask-1", "answers": [ { "id": "q1", "selected": ["测试"] } ] }
```

`answers` 逐题一批：每题恰一项、`id` 与 question 帧的 `questions[].id` 一一
对应。回答经 DSH `userQuestions.answer` **steer 成 user 消息注入 running
turn**——不占忙闩（不判不清 busy），流随即续跑（user/message → 后续
step → turn/end）。三态：`200`（接受）；`404 question not found`（无该
callId 的可回答问题——已答/超时/不存在）；`400`（批结构不合法）；`409
reply queued`（该问题已有排队回复）。

用户不回答时：流停滞 `stallTimeoutMs`（默认 300s）后看门狗报
`turn stalled` 收流；模型侧 timed ask 自带超时也会先收尾。

### tool_result 帧（工具结果外发）

SSE 的 `tool` 帧只发调用（`{name, arguments, callId, seq}`）；配套的
**`tool_result` 帧** `{callId, text, seq}` 把工具结果 text 块拼接外发——
消费方经 `callId` 配对还原「调用 → 读到什么」全过程（对齐 dsh web 渲染）。

> **stream 真流式**：delta/reasoning 帧逐 token 转发——订阅 DSH 的
> `agent/assistant-stream` cordis 事件（agent 作用域、process-local，即 web UI
> 打字机效果的同一通道）。harness 未发 chunk 只给 assistant/message 全文时，
> 自动降级为差量补推（内容不丢不重），消费方无感知。

## 构建

```sh
cd dsh-http-server
npm install        # 装 devDependencies（typescript 等）
npx tsc            # src/ → lib/（入口 lib/index.js）
```

`lib/` 是提交进仓库的编译产物；git 安装路径直接使用，无需安装期构建。开发时
改动 `src/` 后执行 `npx tsc` 重建。

## 挂载方式

插件经 `cordis.patch.yml`（bundle 层）把行插入 DSH 的组合树。三种途径：

### 1. `--patch` 本地调试（不动 profile 安装态）

`cordis.patch.yml` 的 `name: './lib/index.js'` 相对 patch 文件所在目录解析，
无需改动，直接：

```sh
npx @deepseek-ai/dsh web --patch /absolute/path/to/dsh-http-server/cordis.patch.yml
```

`--patch` 覆盖层支持 HMR 热改配置；健康检查：

```sh
curl -s http://127.0.0.1:3080/api/v1/health
# {"ok":true,"service":"dsh-http-server","version":"0.1.0",...}
```

> Windows 注意：PowerShell 的 `curl` 是 `Invoke-WebRequest` 的 alias（参数不兼容），下文 curl 示例请用 `curl.exe` 执行，或改用 `Invoke-RestMethod`。

### 2. 安装进 profile（`dsh plugin add`）

`package.json` 已声明 `"dsh": {"bundle": {"patch": "./cordis.patch.yml"}}`，
安装后插件行自动加入 profile 的层叠。本地 checkout 安装（相对路径以调用目录锚定）：

```sh
dsh plugin --profile web add /absolute/path/to/dsh-http-server
# 或在仓库根：dsh plugin --profile web add ./dsh-http-server
```

安装后**重启 profile**（bundle 成员关系是启动边界）：

```sh
dsh --profile web        # 或 dsh web
curl -s http://127.0.0.1:3080/api/v1/health   # → 200
```

卸载：`dsh plugin --profile web remove dsh-http-server`。

### 3. git 直装（GitLab 等）

```sh
dsh plugin --profile web add git+https://gitlab.example.com/<group>/dsh-http-server.git
# 或 SSH 形式：git+ssh://git@gitlab.example.com/<group>/dsh-http-server.git
```

`lib/` 构建产物已随仓库入库，安装期无构建脚本——无 pnpm ≥10 的 allowBuilds
门槛，`add` 直接成功。

## standalone 独立服务（两栖挂载）

默认只挂宿主 webServer（与 web UI 同端口 3080）。`standalonePort > 0` 时插件
另起一个独立 `node:http` 监听——**与宿主挂载共用同一个路由处理器**（同一路由
表、同一份会话/事件归档状态），用于需要独立端口只暴露给 Java 系统的最小面。

> 注意：本插件依赖 webServer 服务（非可选注入）。无 web 服务的 profile
> （如 sdk/headless）下插件不会激活；纯 API 部署请使用 web profile
> （`dsh web --no-open`，Docker 内同理）。standalone 端口是 web profile
> 内的**额外**监听，不是脱离 web profile 的运行模式。

```yaml
# cordis.patch.yml（patch 层替换整行 config，非深合并）
- insert:
    - id: dsh-http-server
      name: './lib/index.js'       # 相对本 patch 文件所在目录解析
      config:
        standalonePort: 3100        # 0 = 禁用（默认）
        standaloneHost: 127.0.0.1
```

双开时两端口等价：

```sh
curl -s http://127.0.0.1:3080/api/v1/health   # 宿主 → 200
curl -s http://127.0.0.1:3100/api/v1/health   # standalone → 200
```

插件卸载/DSH 退出后两端口均不可达。standalone 端口被占（EADDRINUSE）时插件
只记错误日志并跳过该监听，不影响宿主进程与其他挂载。

## 配置参考

| 字段 | 默认 | 说明 |
|---|---|---|
| `pathPrefix` | `/api/v1` | API 路由前缀（宿主与 standalone 共用） |
| `standaloneHost` | `127.0.0.1` | standalone 监听地址（`standalonePort > 0` 时生效） |
| `standalonePort` | `0` | standalone 端口，`0` = 禁用 |
| `agentPreset` | `standard` | 建会话挂载的工具面 preset id |
| `provider` / `model` | 空 | 缺省模型路由；留空跟随 web UI 默认模型 |
| `reasoningEffort` / `maxTokens` | 空 | 缺省生成参数 |
| `workspaceRoot` | 进程 cwd | 会话 cwd 沙箱根（resolve 后必须位于其下） |
| `stallTimeoutMs` | `300000` | SSE 停滞看门狗（无任何会话事件持续该时长才收流） |

## 冒烟验证（DSH 机器 / Windows 端）

脚本在本插件包 `scripts/` 下（`dsh-http-server/scripts/`），对已挂载插件的真实
DSH 实例断言（`DSH_BASE` 默认 `http://127.0.0.1:3080`；本机无 DSH 时自动落
stub 模式或 SKIP，退出码 0）。以下命令在 `dsh-http-server/` 目录内执行：

```sh
node scripts/smoke-health.mjs        # health + 404 契约（不可达时 SKIP）
node scripts/smoke-sessions.mjs      # 会话/prompt/409/404/400/events/wait/delete
node scripts/smoke-stream.mjs        # SSE 帧序列/心跳/看门狗/409/断开门闩
node scripts/e2e-smoke.mjs           # 全链路（见下）
node scripts/smoke-standalone.mjs    # 双开验证（需先配 standalonePort: 3100）
```

预期输出（真实实例上）：每脚本逐项 `PASS`，结尾 `ALL PASS (n/n)`。各脚本断言集：

- **smoke-health**（2 项）：health 200 固定 JSON；未知路径 404。
- **smoke-sessions**（stub 10 项 / 真实 8 项，第 9/10 项需 stub 故障注入、真实实例自动 SKIP）：
  建会话 201；同步 prompt completed；忙时 409；events 含 turn/end 与 usage；
  `?since=` 增量；wait；DELETE 204 后再 prompt（**双模式**：stub 无持久化 → 404；
  真实持久化会话 → 200 自动 resume，`status` completed/error）；cwd 逃逸 400；
  tasks 抛错后门闩已清；win32 cwd 模拟。
- **smoke-stream**（stub 21 项 + 真实可达时追加 5 项 R1-R5，共 26 项）：
  connected/delta/reasoning/tool/turn/end(usage)/done 帧序列与收流；
  **token 流经 `agent/assistant-stream` 通道注入（贴真实 DSH），逐 chunk →
  逐 delta 帧**；tool 帧含 callId + **tool_result 帧外发工具结果**（callId
  配对）；**question 帧（ask_user_question 结构化外发）+ answer 端点全链路
  （Q1-Q4：question 渲染 → 回答 → 流续跑收尾 → 三态 404/400/409）**；
  无 chunk 全文差量补推；停滞看门狗；忙时 409；客户端断开门闩已清；心跳基建；
  400/404；turn 归属；会话级 error 即时收流；真实流 **R1 断言逐 token
  delta > 1 帧（真流式标准）**/409/400/404/断开复验。
- **e2e-smoke**（9 项）：health → 建会话 → 需工具长任务 prompt → events 轮询
  （含 tool/call）→ 增量游标 → wait → stream 另一轮 → DELETE 204 → 同 id prompt
  复验（stub → 404；真实持久化 → 200，error 不算通过）→
  跨重启恢复。**跨重启恢复为两段式**：第一轮跑完记下输出末尾的 `sessionId`，
  重启 DSH 后执行：

  ```sh
  DSH_RESUME_SESSION=<sessionId> node scripts/e2e-smoke.mjs
  # E9 真实跨重启恢复：DSH_RESUME_SESSION 同 id prompt → 200 completed（自动 resume）
  ```

- **smoke-standalone**（stub 模式 10 项）：双端口 health 均通；响应体完全一致；
  插件卸载后 standalone 不可达、宿主路由已摘 404；宿主退出后两端口均关；
  端口被占（EADDRINUSE）时 apply 不抛、宿主不受影响、错误经日志记录、
  卸载不挂。真实模式：`DSH_BASE` 与 `DSH_STANDALONE_BASE`（默认
  `http://127.0.0.1:3100`）各 health 200，「卸载/退出/端口冲突」由人工
  重启 DSH 验证。

### 错误码速查（Java 侧对接）

| HTTP | body | 语义 |
|---|---|---|
| 409 | `{"error":"session busy"}` | 同会话已有在跑任务（不排队，稍后重试） |
| 404 | `{"error":"not found"}` | 会话不存在且不可恢复 |
| 400 | `{"error":"invalid cwd"}` / `{"error":"invalid prompt"}` | cwd 逃逸沙箱 / body 缺 prompt |
| 400 | `{"error":"invalid body"}` / `{"error":"invalid answer"}` / `{"error":"invalid answer batch"}` | answer body 非法 JSON / 结构不合法 / 批不匹配（每题恰一项） |
| 404 | `{"error":"question not found"}` | 无该 callId 的可回答问题（已答/超时/不存在） |
| 409 | `{"error":"reply queued"}` | 该问题已有排队回复，勿重复提交 |
| 501 | `userQuestions service unavailable` | 宿主未提供 userQuestions 服务（非 web profile 组合） |
