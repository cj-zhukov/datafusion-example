use std::sync::Arc;

use datafusion::prelude::*;
use datafusion::arrow::array::{Int32Array, StringArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;

use datafusion_example::utils::utils::*;

#[test]
fn test_get_column_names() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    ).unwrap();

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch).unwrap();
    let cols = get_column_names(df);
    assert_eq!(cols, vec!["id", "name", "data"]);
}

#[tokio::test]
async fn test_select_all_exclude() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("pkey", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    ).unwrap();

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch).unwrap();
    let res = select_all_exclude(df, &["pkey", "data"]).unwrap();
    
    assert_eq!(res.schema().fields().len(), 2); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)]).unwrap();
    assert_batches_eq!(
        &[
              "+----+------+",
              "| id | name |",
              "+----+------+",
              "| 1  | foo  |",
              "| 2  | bar  |",
              "| 3  | baz  |",
              "+----+------+",
        ],
        &rows.collect().await.unwrap()
    );
}