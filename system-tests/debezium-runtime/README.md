# Debezium runtime bundle probe

这里验证四个受支持 Unix target 的真实内嵌 JVM，不连接外部数据库。
`host` 是不可发布的验收 binary；`probe` 提供确定性的测试 connector 和发布验收脚本。

`bundled_runtime_probe` 在打开 JVM 前注册宿主 Ctrl-C handler，然后用 `/bin/kill` 向自身发送
SIGINT，并要求在 5 秒内收到 handler 通知。JVM 若重新接管信号，probe 会异常退出或超时失败。
首次无效 bundle 拒绝后，四个线程同时打开真实 bundle，要求全部成功；随后验证同路径和符号链接别名复用、不同存在目录优先配置冲突，以及缺失路径和普通文件拒绝。测试路径由 `tempfile` 隔离，不修改真实 bundle。
之后继续验证 start、poll、丢弃 Delivery 后按原 topic/value 与 checkpoint 字节重投、ACK、stop 和 checkpoint-only restart。
probe connector 仍发出 key、Kafka partition、timestamp 和 headers，但 Rust 不导出这些字段；它们不影响 source offset 的推进。
成功必须由宿主正常返回退出码 0。

```bash
cargo build --release -p dogpaddle-debezium-runtime-host
system-tests/debezium-runtime/probe/build.sh
system-tests/debezium-runtime/probe/verify-bundle.sh \
  TARGET ARCHIVE /absolute/path/to/bundled_runtime_probe \
  /absolute/path/to/dogpaddle-debezium-lifecycle-probe.jar SCRATCH_DIR
```

`verify-bundle.sh` 解压到新目录并安装测试 connector，然后清空 PATH、隐藏系统 Java 执行 probe。
`verify-release.sh` 对产品 archive 复用同一 probe，并额外验证产品默认 runtime 路径及 build/reopen。
不要把测试 connector 装入生产或其他验收正在使用的 bundle。普通 Cargo gate 只编译 host；真实 JVM
验收由 runtime bundle/release workflow 调用上述脚本。

同一个 probe 可单独观测重复打开的成本：

```bash
/absolute/path/to/bundled_runtime_probe /absolute/path/to/runtime-bundle --measure-open
```

此模式先初始化真实 JVM 一次，再记录 32 次同路径 `DebeziumRuntime::open` 的纳秒样本；计时只包括调用，初始化、输出和句柄 drop 均在计时外。它不运行 connector 生命周期或默认模式的错误断言，供旧、新产品使用相同 host 源码比较，不代替完整 bundle probe。
