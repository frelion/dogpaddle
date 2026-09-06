# 无声持续 ETL 演示

[观看视频](../assets/fulfillment-continuous.mp4)

画面固定为三栏：**源表 → 完整 SQL 文件 → 目标表**。源表持续执行 28 次真实的新增、修改和删除，
结果表随之变化。付款状态、数量、单价、折扣和地区反复更新，让筛选、重算、分级、路由和撤回的
规律逐渐显现。视频没有音轨，也不切换章节或隐藏 SQL。

黄色标出源字段变化，绿色标出结果新增或更新，红色划线短暂标出刚删除或撤回的行。
表格下方保留本轮旧值到新值的变化，底部显示实际执行的源表 SQL。中间展示
完整的 `fulfillment.sql` 原文，包括读写端点、连接参数、全部 CTE、字段和 `UNION ALL` 分支。
文件仅调整空白排版为 46 行，带行号一次展示到底，全程固定，无省略或滚动。

## 录制

需要 Rust 1.96、Python 3.9+、`uv`、本机 PostgreSQL 可执行文件，以及
[固定版本的 Debezium runtime bundle](../../crates/debezium/README.md#runtime-bundle)。
macOS 使用系统 Menlo 和 Hiragino Sans GB；Linux 使用 DejaVu Sans Mono 或 Liberation Mono，
加 Noto Sans CJK。画面和视频编码只使用 Pillow 与 FFmpeg。

在仓库根目录运行：

```sh
docs/tools/record_fulfillment_demo.sh \
  --bundle /absolute/path/to/runtime-bundle \
  --postgres-bin /absolute/path/to/postgresql/bin
```

脚本编译验收宿主并创建独立的本地 PostgreSQL 临时集群。先执行
[SQL 与崩溃恢复验收](../../system-tests/postgres/check_sql.py)，再在恢复后的同一个 Flow 中持续写入
28 次变更。每次都将完整目标关系与独立执行的 PostgreSQL 原生 SQL 结果比较。
只有全部检查通过且集群停止后，才导出记录并生成视频。

视频只展示末尾的连续更新，恢复过程的原始证据仍保留在 trace 中。

| 产物 | 内容 |
| --- | --- |
| `docs/assets/fulfillment-continuous.mp4` | 2560 × 1440，无音轨，默认 3 分 04 秒 |
| `docs/assets/fulfillment-hero.png` | 持续更新画面中的一帧 |
| `target/demo/fulfillment-trace.json` | 完整 SQL、原始时间戳、宿主响应、恢复证据及连续更新快照 |
| `target/demo/fulfillment-transcript.txt` | 可阅读的 SQL 和宿主执行记录 |
| `target/demo/fulfillment-timeline.json` | 每次变更的播放起点和实际 SQL |

每次源表写入都采集三个时点：写入前；写入后但尚未推进 Flow；推进并结算后。
渲染器直接使用这些源表和目标表快照，不执行 ETL 表达式，不生成业务结果。
源表先变化、目标表随后变化的停留时间为观察需要而设置，不代表实际 CDC 延迟。
内部 oracle 查询不逐条录入 transcript，原始采集时间保存在 trace 中。

## 只重新渲染

```sh
uv run --script docs/tools/render_fulfillment_demo.py \
  --trace target/demo/fulfillment-trace.json \
  --poster docs/assets/fulfillment-hero.png \
  --video docs/assets/fulfillment-continuous.mp4 \
  --transcript target/demo/fulfillment-transcript.txt
```

`--interval 6` 控制每次变更的呈现秒数，范围为 4–15，默认 6；前后各留 8 秒观察。
可用 `--font /absolute/path/to/monospace.ttf` 和 `--caption-font /absolute/path/to/chinese.ttf`
指定字体。视频始终无声，不依赖语音合成或音频文件。

渲染器会拒绝缺少成功标志、连续快照不衔接、源表 SQL 与记录不一致，或推进 Flow 前目标表
已经变化的记录。完整业务正确性由系统验收中的 PostgreSQL oracle 保证。
