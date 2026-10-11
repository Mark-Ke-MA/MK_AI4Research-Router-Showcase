# 验证记录与限制

## 复现所选测试

在已安装 Go 1.26+、Python 3、C 编译器和系统 SQLite 的 macOS 环境中：

```sh
python3 scripts/test.py
```

脚本设置 `GOPROXY=off`、`GOSUMDB=off`、`GOTOOLCHAIN=local` 和 `GOWORK=off`，使用一次性 Go 缓存及测试状态，通过临时 telemetry 目录关闭本次测试进程的 Go telemetry，并仅在本仓库执行 `go test -json -count=1 ./internal/...`。不请求外部 API、不配置账号、不启动 daemon。历史接口测试会绑定临时本地 Unix socket；执行沙盒可能禁止这项操作。

在这种沙盒内，可以明确缩小验证范围：

```sh
python3 scripts/test.py --without-sockets
```

该参数仅排除 `TestHistoryServer*` 的五个顶层测试。协议编码及所有队列／store／规范化／correlation／activity／文件系统测试仍会执行。这是显式限定范围，不是把传输检查失败藏起来的自动 fallback。

## 已验证范围

在 macOS 环境中，显式 `--without-sockets` 运行得到 **63 个通过事件、零失败**，计数包含嵌套 subtest。七个包有测试，model 包无测试。

五个顶层 `TestHistoryServer*` 测试因执行环境限制本地 socket 绑定而未完成验证；原测试完整保留，可在允许绑定的环境中运行完整命令。过滤测试不会计为 skipped 事件。

## 测试证据对应关系

| 被验证的主张 | 代表性原始测试 |
| --- | --- |
| 重复输入事件不会再次创建排队任务 | `TestDuplicateEventAndFIFO` |
| 确认未提交的失败可以重试 | `TestCertifiedPreSubmitCanRetry` |
| 不确定的已提交 turn 不会自动重发 | `TestUncertainNeverResubmits` |
| 不确定的投递不会自动重复 | `TestDeliveryUncertainIsNotRetried` |
| GUI 租约影响本地任务领取 | `TestExternalGUIRacePolicy` |
| 错误 thread／turn 不会被当作答案 | `TestThreadTurnCorrelation` |
| epoch 追加与 alias 修改共享事务结果 | `TestEpochAppendFailureRollsBackAliasCAS` |
| 迁移不会编造更早的历史 | `TestSchemaFourMigrationCreatesHonestBaseline` |
| reaction 清理不会高频冲击故障平台 | `TestFeishuDeleteFailureBacksOffWithoutStorm` |
| 私有路径拒绝符号链接或过宽权限状态 | `TestRejectsUnsafeAndSymlink` |

原始测试使用临时数据库与合成 fake。通过证明的是这些本地约定，不代表在线 Codex 协议兼容性、平台可用性、生产健康、部署安全、负载能力或端到端 exactly-once。本仓库不声称测量过性能指标。
