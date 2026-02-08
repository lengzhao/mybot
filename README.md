# mybot
Personal AI assistant. Enable Control Computer.

## 特性

- **统一消息模型**：定义了标准的 Message 结构，支持文本、图片、文件等多种消息类型
- **灵活适配器架构**：通过适配器接口轻松集成各种平台（Console、DeepSeek、WebChat等）
- **智能路由分发**：支持点对点路由和默认路由策略
- **状态存储系统**：自动记录消息历史，提供查询和统计功能
- **Web管理界面**：通过浏览器查看和管理消息存储
- **配置驱动**：通过 YAML 配置文件灵活管理适配器和系统设置
- **模块化设计**：清晰的代码结构，易于扩展和维护

## 快速开始

### 安装依赖
```bash
go mod tidy
```

### 配置文件
复制配置示例文件：
```bash
cp config.yaml.example config.yaml
```

### 启用完整功能（可选）
在 config.yaml 中添加：
```yaml
system:
  state_store:
    enabled: true
    db_path: "./data/state.db"
    auto_cleanup: true
    cleanup_days: 30
  admin:
    enabled: true
    port: "8081"
```

或者直接使用完整配置示例：
```bash
cp config.yaml.full.example config.yaml
```

### 运行
```bash
go run .
```

## 适配器

目前支持的适配器：
- **Console**：终端交互适配器
- **DeepSeek**：DeepSeek AI 大模型适配器
- **WebChat**：Web 聊天界面适配器
- **OpenCode**：代码执行适配器

## 管理服务

独立的管理服务提供 Web 界面和 API 接口：

### Web 管理界面
访问 `http://localhost:8081` 查看管理界面

### API 接口
- `GET /api/messages` - 查询消息列表
- `GET /api/stats` - 获取统计信息
- `GET /health` - 健康检查

## StateStore 状态存储

StateStore 提供了完整的消息历史记录和管理功能，由管理服务调用展示。

详细文档请查看 [docs/StateStore文档.md](docs/StateStore文档.md)

## 开发

### 项目结构
```
.
├── adapters/          # 适配器实现
├── admin/            # 管理服务
│   ├── static/       # 静态文件 (HTML/CSS/JS)
│   └── service.go    # 管理服务实现
├── sqlite.go         # GORM数据库实现
├── docs/             # 文档
├── config.go         # 配置管理
├── dispatcher.go     # 消息分发器
├── types.go          # 核心类型定义
└── todo.md           # 待办事项
```

### 添加新适配器
1. 在 adapters/ 目录下创建新包
2. 实现 Adapter 接口
3. 在 init() 函数中注册适配器类型
4. 在配置文件中添加适配器配置

## 许可证
MIT