# StateStore 状态存储系统文档

## 概述

StateStore 是一个基于 SQLite 的状态存储系统，用于自动记录和管理消息转发历史。它提供了完整的消息查询、统计分析和 Web 管理界面功能。

## 功能特性

1. **自动记录**：每次消息转发时自动记录到 SQLite 数据库
2. **完整查询**：支持按多种条件过滤查询消息历史
3. **统计分析**：提供消息量、适配器使用情况等统计信息
4. **Web 管理界面**：通过浏览器查看和管理存储的数据
5. **自动清理**：可配置自动清理过期数据
6. **灵活配置**：支持启用/禁用和各种配置选项

## 配置

在 `config.yaml` 中添加 StateStore 配置：

```yaml
system:
  state_store:
    enabled: true           # 是否启用状态存储
    db_path: "./data/state.db"  # 数据库文件路径
    auto_cleanup: true      # 是否自动清理旧数据
    cleanup_days: 30        # 自动清理多少天前的数据
```

## API 接口

### Web 管理界面
- `GET /admin` - 管理页面

### REST API
- `GET /api/messages` - 查询消息列表
- `GET /api/stats` - 获取统计信息

### 消息查询参数
- `source_adapter` - 源适配器ID
- `target_adapter` - 目标适配器ID
- `user_id` - 用户ID
- `channel` - 频道ID
- `type` - 消息类型
- `start_time` - 开始时间戳
- `end_time` - 结束时间戳
- `limit` - 限制返回数量
- `offset` - 偏移量

## 使用示例

### 启用 StateStore

```go
// 创建配置
config := mybot.StateStoreConfig{
    Enabled:     true,
    DBPath:      "./data/state.db",
    AutoCleanup: true,
    CleanupDays: 30,
}

// 创建 StateStore 实例
stateStore, err := mybot.NewSQLiteStore(config)
if err != nil {
    log.Fatal(err)
}

// 启动
if err := stateStore.Start(); err != nil {
    log.Fatal(err)
}
defer stateStore.Stop()

// 在 Dispatcher 中集成
dispatcher.SetStateStore(stateStore)
```

### 查询消息

```go
// 查询最新50条消息
filter := mybot.MessageFilter{
    Limit: 50,
}
messages, err := stateStore.QueryMessages(filter)

// 按条件过滤查询
filter = mybot.MessageFilter{
    SourceAdapter: "console",
    UserID:       "test-user",
    Limit:        100,
}
messages, err = stateStore.QueryMessages(filter)
```

### 获取统计信息

```go
stats, err := stateStore.GetStats()
if err != nil {
    log.Fatal(err)
}

fmt.Printf("总消息数: %d\n", stats.TotalMessages)
fmt.Printf("各适配器消息数: %v\n", stats.MessagesByAdapter)
fmt.Printf("各用户消息数: %v\n", stats.MessagesByUser)
```

## 数据结构

### MessageRecord
```go
type MessageRecord struct {
    ID            string                 `json:"id"`
    SourceAdapter string                 `json:"source_adapter"`
    TargetAdapter string                 `json:"target_adapter"`
    UserID        string                 `json:"user_id"`
    Channel       string                 `json:"channel"`
    Content       string                 `json:"content"`
    Type          string                 `json:"type"`
    Timestamp     int64                  `json:"timestamp"`
    Files         []File                 `json:"files"`
    Extra         map[string]interface{} `json:"extra"`
    ProcessedAt   int64                  `json:"processed_at"`
}
```

### Stats
```go
type Stats struct {
    TotalMessages     int64            `json:"total_messages"`
    MessagesByAdapter map[string]int64 `json:"messages_by_adapter"`
    MessagesByUser    map[string]int64 `json:"messages_by_user"`
    MessagesByChannel map[string]int64 `json:"messages_by_channel"`
    MessagesByType    map[string]int64 `json:"messages_by_type"`
}
```

## 注意事项

1. **性能考虑**：大量消息存储时建议定期清理旧数据
2. **磁盘空间**：SQLite 数据库文件会持续增长，需要监控磁盘使用情况
3. **并发访问**：StateStore 实现了并发安全，但大量并发查询可能影响性能
4. **错误处理**：数据库操作失败时会记录日志但不会中断消息转发流程

## 故障排除

### 常见问题

1. **数据库文件权限问题**
   - 确保配置的 `db_path` 目录有写入权限
   - 检查磁盘空间是否充足

2. **查询性能问题**
   - 合理使用分页参数 (`limit`, `offset`)
   - 避免不加限制的全表查询
   - 定期维护数据库索引

3. **Web 界面访问问题**
   - 确保 webchat 适配器已启用并运行
   - 检查网络连接和端口配置