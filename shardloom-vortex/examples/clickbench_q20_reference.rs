//! Independent reference for the previously descriptor-only Q20 output.
//! Reads official Parquet through Arrow, never through `ShardLoom` query execution.
use arrow_array::{Array as _, Int64Array};
use parquet::arrow::{ProjectionMask, arrow_reader::ParquetRecordBatchReaderBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: clickbench_q20_reference HITS_PARQUET")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
    let index = builder.schema().index_of("UserID")?;
    let projection = ProjectionMask::roots(builder.parquet_schema(), [index]);
    let reader = builder
        .with_projection(projection)
        .with_batch_size(65_536)
        .build()?;
    let mut rows = Vec::new();
    let mut input_rows = 0;
    for batch in reader {
        let batch = batch?;
        input_rows += batch.num_rows();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("expected int64 UserID")?;
        for index in 0..ids.len() {
            if ids.is_valid(index) && ids.value(index) == 435_090_932_899_640_449 {
                if rows.len() >= 65_536 {
                    return Err("reference output row bound exceeded".into());
                }
                rows.push(serde_json::json!({"UserID": ids.value(index)}));
            }
        }
    }
    println!(
        "{}",
        serde_json::json!({
            "schema_version": "shardloom.clickbench.independent_result_reference.v1",
            "query": 20, "input_rows": input_rows, "values": rows,
            "provider": "parquet 58.3 + Arrow typed int64 scalar equality; independent of ShardLoom and Vortex query paths"
        })
    );
    Ok(())
}
