# Cursor 规则模板

新会话 workdir 首次创建时，本目录**内容**拷贝到 **workdir/.cursor/**，不覆盖已有文件。

| 文件 | 说明 |
|------|------|
| AGENTS.md | 能力、文件与内容、写入规则、压缩前刷写 |
| USER.md | 用户信息 |
| TODO.md | 待办 |
| TOOLS.md | 工具约定、MCP、Skills、常用命令 |
| MEMORY.md | 长期记忆 |
| memory/ | 按日 `YYYY-MM-DD.md` 日志 |

**与 Cursor 的配合**：拷贝后规则与记忆在 `.cursor/` 下，Cursor 可识别。可选扩展：`.cursor/mcp.json` 配置 MCP 服务器；`.cursor/rules/*.mdc` 为项目规则（description/globs/alwaysApply）。Skills 为任务型指令，由 Cursor 设置或环境提供。

设计参考：OpenClaw 记忆范式。
