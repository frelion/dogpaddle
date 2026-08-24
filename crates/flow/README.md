# dogpaddle-flow

`dogpaddle-flow` 是 DogPaddle 内部的持久化数据流执行内核。它负责声明静态 DAG、
公平调度 Stage、执行 Operation，并将运行进度与操作状态一起持久化。该 crate 是引擎
实现模块，不是最终面向用户的二进制入口。

## 核心模型

- `Flow` 持有不可变的数据流拓扑和公平调度器。
- `Stage` 持有一个 `Operation`，也是唯一的事务与输出发布边界。
- `Operation` 承载领域语义；其持久化状态只能位于检查点或注入的 Store 集合中。

`Operation::step` 只接收共享的 `Transaction` 借用，因此不能自行开始或提交事务。
每次成功的 Stage 转换会原子提交操作状态、检查点、输出、输入进度和调度进度；
`Decision::Pending` 会回滚整次尝试。先前已经提交的输出不会因后续操作失败而失效。

## 调度与背压

Flow 在首次执行前校验拓扑：图必须无环，每个输入端口必须连接且只能有一个上游。
调度器采用公平轮转，并持久化下一调度位置。每条边最多保留一个待消费输出块；扇出时，
上游会等待最慢的消费者，从而形成有界背压。没有下游的叶子 Stage 不能发布输出。

## 恢复与外部副作用

重新打开 Flow 时，调用方必须重新声明相同的数据空间、拓扑和 Operation 指纹；中断的
初始化也按相同规则恢复。一个 Store 路径同一时间只能有一个活动的 Flow 执行器，释放
Flow 后其他进程才能重新打开。外部副作用不属于 Store 事务；如果崩溃重试不能重复执行，
Operation 必须使用稳定的幂等键。

## 完整示例

下面的 Flow 由一个数据源和一个计数器组成。数据源发布一个块，计数器在同一 Stage
事务中更新持久化状态。

```rust,no_run
use dogpaddle_flow::{
    Decision, Event, Flow, Operation, OperationError, StepOutcome, Work,
};
use dogpaddle_store::{Cell, DataPlacement, Transaction};

struct Source;

impl Operation for Source {
    fn fingerprint(&self) -> &[u8] {
        b"source:v1"
    }

    fn step(
        &mut self,
        _work: Work<'_>,
        _transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        Ok(Decision::Complete {
            output: Some(b"hello".to_vec()),
        })
    }
}

struct Sink {
    count: Cell<u64>,
}

impl Operation for Sink {
    fn fingerprint(&self) -> &[u8] {
        b"sink:v1:count"
    }

    fn step(
        &mut self,
        work: Work<'_>,
        transaction: &Transaction<'_>,
    ) -> Result<Decision, OperationError> {
        if let Event::Data { .. } = work.event() {
            let mut count = self.count.access(transaction)?;
            count.set(&(count.get()?.unwrap_or(0) + 1))?;
        }
        Ok(Decision::Complete { output: None })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut flow = Flow::create("./dogpaddle-data")?;
    let count = Cell::new(flow.data("count", DataPlacement::Shared)?);
    let source = flow.stage("source", &[], Source)?;
    let sink = flow.stage("sink", &["input"], Sink { count })?;
    flow.connect(source, sink, "input")?;

    loop {
        match flow.step()? {
            StepOutcome::Progress => {}
            StepOutcome::Idle => break, // 等待外部唤醒
            StepOutcome::Finished => break,
        }
    }
    Ok(())
}
```

## 测试

运行该 crate 的全部测试：

```bash
cargo test -p dogpaddle-flow
```

执行和拓扑行为是独立的集成测试目标，也可以单独运行：

```bash
cargo test -p dogpaddle-flow --test execution
cargo test -p dogpaddle-flow --test topology
```
