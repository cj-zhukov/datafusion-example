use std::sync::Arc;

use datafusion::prelude::*;
use datafusion::arrow::array::{Array, Int32Array, StringArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;

use datafusion_example::utils::utils::*;
use datafusion_example::df;

#[tokio::test]
async fn test_df_macro() {
    let id = Int32Array::from(vec![1, 2, 3]);
    let data = Int32Array::from(vec![42, 43, 44]);
    let name = StringArray::from(vec!["foo", "bar", "baz"]);

    let df = df!(
        "id" => id,
        "data" => data,
        "name" => name
    );

    assert_eq!(df.schema().fields().len(), 3); // columns count
    assert_eq!(df.clone().count().await.unwrap(), 3); // rows count
    
    let rows = df.sort(vec![col("id").sort(true, true)]).unwrap();
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | data | name |",
            "+----+------+------+",
            "| 1  | 42   | foo  |",
            "| 2  | 43   | bar  |",
            "| 3  | 44   | baz  |",
            "+----+------+------+",
        ],
        &rows.collect().await.unwrap()
    );
}

#[tokio::test]
async fn test_df_sql() {
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
    let sql = r#"id > 2 and data > 43 and name in ('foo', 'bar', 'baz')"#;
    let res = df_sql(df, sql).await.unwrap();

    assert_eq!(res.schema().fields().len(), 3);
    assert_eq!(res.clone().count().await.unwrap(), 1);
    
    let rows = res.sort(vec![col("id").sort(true, true)]).unwrap();
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | name | data |",
            "+----+------+------+",
            "| 3  | baz  | 44   |",
            "+----+------+------+",
        ],
        &rows.collect().await.unwrap()
    );
}

#[tokio::test]
async fn test_is_empty() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    ).unwrap();

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch).unwrap();
    assert_eq!(is_empty(df).await.unwrap(), false);

    let batch = RecordBatch::new_empty(schema.into());
    let df = ctx.read_batch(batch).unwrap();
    assert_eq!(is_empty(df).await.unwrap(), true);
}

#[tokio::test]
async fn test_df_to_table() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),

    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    ).unwrap();

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch).unwrap();
    df_to_table(ctx.clone(), df, "t").await.unwrap();
    let res = ctx.sql("select * from t order by id").await.unwrap();
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | data | name |",
            "+----+------+------+",
            "| 1  | 42   | foo  |",
            "| 2  | 43   | bar  |",
            "| 3  | 44   | baz  |",
            "+----+------+------+",
        ],
        &res.collect().await.unwrap()
    );
}