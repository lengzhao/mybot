# Todo List

- [x] 定义核心消息模型 (Message) 与适配器接口 (Adapter) @complete
- [x] 实现核心调度器 (Dispatcher) @complete
- [x] 实现基础能力适配器 (如 Mock AI 或 Console) @complete
- [x] 实现 DeepSeek LLM 适配器 @complete
- [x] 编写单元测试验证路由逻辑 @complete
- [x] 修复 mockAdapter 缺少 GetDefaultTarget 方法导致的编译错误 @complete
- [x] 实现适配器默认路由配置功能 @complete
- [ ] 实现各平台 SDK 适配器 (Lark, DingTalk 等) @pending
- [x] 重构项目结构：引入 adapters 文件夹与自注册机制 @complete
- [x] 实现配置加载模块 @complete
- [ ] 实现状态/上下文存储 (StateStore) @pending
- [x] 改进 opencode 适配器进程管理：移除不当的 kill 逻辑，添加优雅关闭机制 @complete

