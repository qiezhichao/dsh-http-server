/**
 * dsh-http-server — 插件配置 schema 与同构类型
 */
import z from '@deepseek-ai/schemastery';
export const Config = z.object({
    pathPrefix: z.string().default('/api/v1').description('API 路由前缀'),
    standaloneHost: z.string().default('127.0.0.1').description('standalone 独立服务监听地址（standalonePort > 0 时生效）'),
    standalonePort: z.natural().default(0).description('standalone 独立服务端口，0 = 禁用'),
    agentPreset: z.string().default('standard').description('会话挂载的工具面 preset id'),
    provider: z.string().description('缺省模型 provider，留空跟随 web UI 默认模型'),
    model: z.string().description('缺省模型名称，留空跟随 web UI 默认模型'),
    reasoningEffort: z.string().description('缺省推理力度'),
    maxTokens: z.natural().description('缺省最大生成 token 数'),
    workspaceRoot: z.string().default(process.cwd()).description('会话工作目录根'),
    stallTimeoutMs: z.natural().default(300000).description('SSE 停滞看门狗毫秒数'),
});
//# sourceMappingURL=config.js.map