use std::{env, error::Error, fs, num::NonZeroU64, path::PathBuf, thread, time::Duration};

use arrow_schema::DataType;
use dogpaddle_flow::FlowFactory;
use dogpaddle_operation::{
    cast, col, lit,
    operation::{
        sink::SqliteSinkDefinition,
        source::SequenceSourceDefinition,
        transform::{ExtendDefinition, FilterDefinition, SelectDefinition},
    },
};

const OUTPUT_CAPACITY_BYTES: NonZeroU64 = NonZeroU64::MAX;

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    let root = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("usage: sqlite_sink_live <demo-directory> [rounds] [delay-ms]")?;
    let rounds = arguments
        .next()
        .map(|value| value.into_string().map_err(|_| "rounds must be UTF-8"))
        .transpose()?
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(24);
    let delay_ms = arguments
        .next()
        .map(|value| value.into_string().map_err(|_| "delay-ms must be UTF-8"))
        .transpose()?
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(450);
    if arguments.next().is_some() {
        return Err("too many arguments".into());
    }

    fs::create_dir_all(&root)?;
    let root = root.canonicalize()?;
    let flow_path = root.join("flow");
    let sqlite_path = root.join("events.sqlite");

    let mut factory = FlowFactory::new(&flow_path);
    let source = factory.station("source", SequenceSourceDefinition::new(0));
    let extend = factory.station(
        "extend",
        ExtendDefinition::try_new("number", cast(col("value"), DataType::Int64))?,
    );
    let filter = factory.station(
        "filter",
        FilterDefinition::try_new((col("number") % lit(2_i64)).eq(lit(0_i64)))?,
    );
    let select = factory.station(
        "select",
        SelectDefinition::try_new([
            ("number", col("number")),
            ("square", col("number") * col("number")),
        ])?,
    );
    let sink = factory.station(
        "sqlite",
        SqliteSinkDefinition::try_new(&sqlite_path, "even_squares")?,
    );

    for station in [source, extend, filter, select] {
        factory.output_capacity_bytes(station, OUTPUT_CAPACITY_BYTES);
    }
    factory.connect([source], extend);
    factory.connect([extend], filter);
    factory.connect([filter], select);
    factory.connect([select], sink);

    println!("DogPaddle SQLiteSink live demo");
    println!("pipeline: Sequence -> Extend -> Filter -> Select -> SQLiteSink");
    println!("transform: cast to Int64, keep evens, compute square");
    println!("SQLite: {}", sqlite_path.display());
    println!("display delay: {delay_ms} ms per round (demo only)");
    println!();

    let mut flow = factory.build()?;
    for round in 1..=rounds {
        let outcome = flow.advance()?;
        println!("round {round:02}  flow.advance() -> {outcome:?}");
        thread::sleep(Duration::from_millis(delay_ms));
    }

    println!();
    println!("paused after {rounds} rounds - reopen the Flow to continue");
    println!("rows remain in {}", sqlite_path.display());
    Ok(())
}
