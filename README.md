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
| POST | `/sessions/:id/prompt` | 同步提问 → `200 {status, text, usage?}`（`status`: completed/timeout/error） |
| POST | `/sessions/:id/tasks` | 异步任务 → `202 {taskId}`（完成与否经 events 轮询 turn/end 判定） |
| GET | `/sessions/:id/events?since=N` | 事件增量轮询 → `{events, nextSince, status, lastAssistantText}` |
| POST | `/sessions/:id/wait` | 等待在跑任务收敛 → `200 {status, text, usage?}` |
| POST | `/sessions/:id/stream` | SSE 流式（connected/delta/reasoning/tool/turn/end/done 帧；心跳 15s；停滞看门狗） |
| DELETE | `/sessions/:id` | 处置会话 → `204`（幂等；内存处置语义，见下「跨重启恢复」） |

错误契约：`409 {"error":"session busy"}`、`404 {"error":"not found"}`、
`400 {"error":"invalid cwd"}` / `400 {"error":"invalid prompt"}`。

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
- **smoke-sessions**（10 项）：建会话 201；同步 prompt completed；忙时 409；
  events 含 turn/end 与 usage；`?since=` 增量；wait；DELETE 204 后 404；
  cwd 逃逸 400；tasks 抛错后门闩已清；win32 cwd 模拟。
- **smoke-stream**（17 项）：connected/delta/reasoning/tool/turn/end(usage)/done
  帧序列与收流；无 chunk 全文差量补推；停滞看门狗；忙时 409；客户端断开门闩
  已清；心跳基建；400/404；turn 归属；会话级 error 即时收流。
- **e2e-smoke**（9 项）：health → 建会话 → 需工具长任务 prompt → events 轮询
  （含 tool/call）→ 增量游标 → wait → stream 另一轮 → DELETE 204 → 404 复验 →
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
