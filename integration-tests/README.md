# 外部组合测试

本目录只保存没有自然产品组合根的跨 crate 接缝测试。每个子目录都是工作区内
`publish = false` 的下游 package，只能经产品公共 API 装配真实依赖。

当前 registry：

| package | 所有权 |
| --- | --- |
| [`change-store`](./change-store/) | 完整 Change Stream 与 `AppendLog<Vec<u8>>` 的最小接缝正确性和常规性能 |

Operation 正式依赖 Store，其组合契约归 `crates/operation/tests/correctness/`；Flow 是 Operation +
Store 的组合根，其 build/open/materialize 契约归 `crates/flow/tests/correctness/`。不要为这些正常
依赖重复创建外部 package。未来只有在新接缝没有任何产品 crate 能自然拥有时，才在这里登记。

全工作区目录、依赖和执行规则见根目录 [`TESTING.md`](../TESTING.md)。
