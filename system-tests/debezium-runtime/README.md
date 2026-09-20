# Debezium runtime bundle probe

这里验证四个受支持 Unix target 的真实内嵌 JVM，不连接外部数据库。
`host` 是不可发布的验收 binary；`probe` 提供确定性的测试 connector 和发布验收脚本。

`bundled_runtime_probe` 在打开 JVM 前注册宿主 Ctrl-C handler，然后用 `/bin/kill` 向自身发送
SIGINT，并要求在 5 秒内收到 handler 通知。JVM 若重新接管信号，probe 会异常退出或超时失败。
之后继续验证 start、poll、丢弃 Delivery 后重投、ACK、stop 和 checkpoint-only restart。
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
