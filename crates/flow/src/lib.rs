#![doc = include_str!("../README.md")]

#[expect(
    dead_code,
    reason = "the private topology core will be wired into the durable Flow builder"
)]
mod topology;
