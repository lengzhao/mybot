# WebChat 适配器

WebChat 适配器是一个基于Web的聊天界面，允许用户通过浏览器与机器人进行交互。

## 功能特性

- 基于Web的实时聊天界面
- 会话管理（支持多个并发会话）
- RESTful API 接口
- WebSocket 实时双向通信
- 消息历史记录
- 健康检查端点

## 配置选项

在 `config.yaml` 中配置 WebChat 适配器：

```yaml
adapters:
  - id: "webchat"
    type: "webchat"
    enabled: true
    default_target: "echo"      # webchat的回复默认发送回echo或其他指定适配器
    config:
      port: 8080              # Web服务器端口
```

## API 端点

- `GET /` - 访问聊天界面
- `POST /send` - 发送消息（HTTP方式）
- `GET /health` - 健康检查
- `GET /sessions` - 获取活跃会话列表
- `GET /ws` - WebSocket 连接（实时双向通信）

## 使用方法

1. 启动机器人服务
2. 访问 `http://localhost:8080`（或您配置的端口）
3. 在聊天界面中输入消息
4. 查看机器人的回复

## 消息格式

发送消息的 POST 请求格式：
```json
{
  "session_id": "unique_session_identifier",
  "content": "your message here",
  "extra": {}
}
```

## 注意事项

- 静态文件通过 `embed` 包直接嵌入到二进制文件中，无需额外部署
- 会话数据会在 30 分钟不活动后自动清理
- 消息发送超时时间为 5 秒