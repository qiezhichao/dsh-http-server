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

import * as nodeHttp from 'node:http'
import type { Context } from '@deepseek-ai/cordis'
import type { WebRoute } from '@deepseek-ai/dsh-host-webserver'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { type HttpServerConfig, Config } from './config.js'
import { type MatchDeps, match, requestPath, stripPrefix } from './router.js'
import { EventArchive, attachEventArchive } from './events.js'
import { SessionStore } from './sessions.js'

export { Config }

export const name = 'dsh-http-server'

/**
 * agents/workingDirectory/llm/webServer 为必需服务。
 * 注意：@deepseek-ai/cordis 4.1 的 Inject.resolve 不剥离 '?' 后缀——数组里的
 * 'webServer?' 会被当作字面服务名等待，导致插件永远 pending（Windows 实测）。
 * 因此全部用非可选注入；cordis 会在服务就绪后自动激活（_refresh 机制）。
 * agentDefaultModel 不入注入表：web profile 未必稳定提供，apply 内经
 * ctx.get('agentDefaultModel') 动态探测（缺省回退 config.provider/model）。
 */
export const inject = ['agents', 'workingDirectory', 'llm', 'webServer']

export function apply(ctx: Context, config: HttpServerConfig): void {
  const prefix = config.pathPrefix || '/api/v1'

  // 任务 3 装配：事件归档 + 会话登记簿（同一份依赖注入路由与归档订阅）。
  // 归档只收本插件在册会话的事件（store.get 判定）——宿主外全会话不进归档（审查 I-3）。
  const archive = new EventArchive()
  const store = new SessionStore(ctx, config, archive)
  const deps: MatchDeps = { ctx, config, store, archive }
  attachEventArchive(ctx, archive, (sessionId) => store.get(sessionId) !== undefined)

  // 插件卸载时释放全部 agent 句柄（dispose 尽力清，失败不阻塞卸载）。
  ctx.effect(() => () => { void store.disposeAll() }, 'dsh-http-server: dispose sessions on unload')

  /** 前缀路由处理器：剥前缀后交给 match 分发。 */
  const routeHandler = (req: IncomingMessage, res: ServerResponse): void => {
    match(req, res, stripPrefix(requestPath(req), prefix), deps)
  }

  // 宿主 0.2.2-rc.5 的 WebRoute 类型尚无 auth 字段（master 源码已有，缺省 'required'）；
  // 显式标注 anonymous 以兼容 master 语义，rc.5 运行时忽略该字段。
  const route: WebRoute & { auth: 'anonymous' } = {
    kind: 'prefix',
    path: prefix,
    auth: 'anonymous',
    handler: routeHandler,
  }

  // 挂宿主 webServer（非可选注入——web profile 必有；服务就绪后 cordis 自动激活本插件）
  ctx.effect(() => ctx.webServer.register(route), 'dsh-http-server: webServer prefix route')

  // standalone 独立服务（任务 5 两栖挂载）：standalonePort > 0 时起独立
  // node:http 监听，与宿主挂载共用同一个 routeHandler——同一路由表、同一份
  // SessionStore/EventArchive，两端口行为完全一致。
  if (config.standalonePort > 0) {
    ctx.effect(() => {
      const server = nodeHttp.createServer((req: IncomingMessage, res: ServerResponse) => routeHandler(req, res))
      let closed = false
      const close = (): Promise<void> => {
        if (closed) return Promise.resolve()
        closed = true
        return new Promise<void>((resolve) => { server.close(() => resolve()) })
      }
      // 'error' 兜底（审查 I-1）：listen 失败（如 EADDRINUSE）若无监听器是
      // uncaught error 事件——直接击穿整个 DSH 宿主进程。参照 dsh-host-webserver
      // 的 listen error 记录形态，但作为插件不能拖垮宿主：记日志 + 关掉失败
      // server（close 幂等），apply 正常返回——宿主挂载照常，卸载 disposer
      // 对已关 server 也是安全 no-op。运行期 error 事件同样只记日志不外抛。
      server.on('error', (error: NodeJS.ErrnoException) => {
        ctx.logger.error(`dsh-http-server standalone ${config.standaloneHost}:${config.standalonePort} failed: ${error.code ?? ''} ${error.message}`)
        void close()
      })
      server.listen(config.standalonePort, config.standaloneHost)
      // 卸载时 server.close 收口（effect 清理函数返回 Promise，cordis 卸载流程
      // 会 await 它；close 幂等——listen 失败路径已关过一次）。
      return () => close()
    }, 'dsh-http-server: standalone http server')
  }
}
