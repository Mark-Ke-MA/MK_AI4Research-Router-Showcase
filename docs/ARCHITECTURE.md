# 架构与职责边界

中文 | [English](../docs_en/ARCHITECTURE.md)

## 围绕上游 runtime 构建的持久化消息桥

私人系统中，每个人格有独立的 Router、数据库、app-server 连接、Codex home 和平台绑定。数据库维护哪个 thread 是 canonical `main`；展示标题不决定身份。预定会话不可用时，平台适配器不能因此擅自新建替代会话。

Router 处理认证后的消息桥输入。桌面／手机原生入口直接使用上游 app-server 能力。自建的外部 turn 观察器将已知 GUI 活动转成 alias 租约，让消息桥 worker 在观察到活动期间推迟领取新任务。

| 层次 | 能力来源 | 本仓库包含什么 |
| --- | --- | --- |
| 模型推理、工具执行、原生 thread 协议、沙盒 | 上游 Codex 与模型服务 | 不含实现；测试使用 fake client |
| Telegram／飞书传输和认证 | 平台 API／SDK 与自建适配器 | 仅规范化逻辑；不含在线传输 |
| owner／endpoint 绑定、排序、去重、租约、outbox | 自建 Router | 实现与测试 |
| SQLite 事务、WAL 与持久化原语 | SQLite | cgo 调用系统库；不附 SQLite 源码 |
| main 身份、generation 转换与 epoch 账本 | 基于 Codex thread 的自建控制／存储机制 | 存储事务与历史；不含 owner 操作程序 |
| 权限 profile、release 绑定、启动与运行前置检查 | 自建策略／发布集成，加 OS 与上游执行约束 | 仅提供架构说明 |
| 长期记忆选择与索引 | MQElite | 独立展示仓库 |

保护 main 的 guardian 和 owner 专用轮换工具属于私人系统，但不在本次代码选择中。它们将普通消息与更换主会话的权限分开。轮换要求状态静止，并在同一事务内提交 alias、generation 与 epoch 历史。由于 app-server 不提供原子的远端输入闸门，仍存在短暂的远端竞态窗口，需要 owner 在操作期间协调输入。这不意味着数据库事务能够原子锁住所有 GUI 输入。

## 实体代码结构

| 包／文件 | 职责 |
| --- | --- |
| [`adapters`](../internal/adapters/adapters.go) | 规范化已认证事件，约束输入大小 |
| [`model`](../internal/model/model.go) | 任务状态、类型化失败边界及共享记录 |
| [`store`](../internal/store/store.go) | SQLite 事务、去重、队列／outbox、租约与生命周期记录 |
| [`store/main_history.go`](../internal/store/main_history.go) | 已提交 epoch、迁移覆盖边界与完整性验证 |
| [`router`](../internal/router/router.go) | 接收输入、worker、提交边界与投递 |
| [`router/external.go`](../internal/router/external.go) | 将已知外部 turn 转成 alias 租约 |
| [`appserver`](../internal/appserver/correlation.go) | 同时按 thread ID 和 turn ID 匹配通知 |
| [`activity`](../internal/activity/activity.go) | 有界的 typing／reaction 提示生命周期 |
| [`mainhistory`](../internal/mainhistory/server.go) | 私有 Unix socket 上的有界只读历史协议 |
| [`securefs`](../internal/securefs/securefs.go) | 私有权限模式、符号链接与原子替换检查 |

这些包之间的依赖都在仓库内。保留原 module 名是为了不修改 import 路径，不表示它是已经发布的 Go module 地址。

## 适用边界

代码面向 Unix／macOS，本次只在 macOS 测试。原始 cgo 链接指令针对 Darwin，因此不能把这份提取视为已验证的 Linux 可移植包。测试中的 owner、thread、endpoint 和密钥均为合成值。仓库不含私人配置、真实消息、账号绑定、会话转录、凭证、部署二进制或在线数据库。

直接文件检查加强的是应用自身的状态处理，不创造新的 OS 沙盒，也不保护同一登录用户下能够访问状态及密钥的恶意进程。HMAC 链在密钥假设下检测不一致，不是由独立第三方见证的审计日志。
