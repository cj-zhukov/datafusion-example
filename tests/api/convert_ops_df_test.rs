use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{
    Array, Float64Array, Int32Array, LargeStringArray, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::assert_batches_eq;
use datafusion::prelude::*;
use serde_json::{Map, Value};

use datafusion_example::utils::utils::*;

use crate::helpers::*;

#[test]
fn test_convert_cols_to_json() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let a = Int32Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

    let buf = vec![];
    let mut writer = arrow_json::ArrayWriter::new(buf);
    writer.write(&batch)?;
    writer.finish()?;

    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;

    assert_eq!(
        serde_json::Value::Object(json_rows[1].clone()),
        serde_json::json!({"a": 2}),
    );

    Ok(())
}

#[tokio::test]
async fn test_concat_arrays() -> Result<()> {
    let df = get_df1()?;

    let arrays = concat_arrays(df).await?;
    assert_eq!(arrays.len(), 3);

    let ids = arrays
        .get(0)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2, 3]);

    let names = arrays
        .get(1)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "foo");
    assert_eq!(names.value(1), "bar");
    assert_eq!(names.value(2), "baz");

    let data_all = arrays
        .get(2)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(data_all.values(), &[42, 43, 44]);

    Ok(())
}

#[tokio::test]
async fn test_cols_to_json() -> Result<()> {
    let df = get_df1()?;

    let ctx = SessionContext::new();
    let res = df_cols_to_json(&ctx, df, &["name", "data"], "metadata").await?;

    assert_eq!(res.schema().fields().len(), 2); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+--------------------------+",
            "| id | metadata                 |",
            "+----+--------------------------+",
            r#"| 1  | {"data":42,"name":"foo"} |"#,
            r#"| 2  | {"data":43,"name":"bar"} |"#,
            r#"| 3  | {"data":44,"name":"baz"} |"#,
            "+----+--------------------------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_cols_to_struct() -> Result<()> {
    let df = get_df1()?;

    let ctx = SessionContext::new();
    let res = df_cols_to_struct(&ctx, df, &["name", "data"], "metadata").await?;

    assert_eq!(res.schema().fields().len(), 2); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+-----------------------+",
            "| id | metadata              |",
            "+----+-----------------------+",
            r#"| 1  | {name: foo, data: 42} |"#,
            r#"| 2  | {name: bar, data: 43} |"#,
            r#"| 3  | {name: baz, data: 44} |"#,
            "+----+-----------------------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_pk_to_df() -> Result<()> {
    let df = get_df1()?;

    let ctx = SessionContext::new();
    let res = add_pk_to_df(&ctx, df, "pk").await?;

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+----+",
            "| id | name | data | pk |",
            "+----+------+------+----+",
            "| 1  | foo  | 42   | 0  |",
            "| 2  | bar  | 43   | 1  |",
            "| 3  | baz  | 44   | 2  |",
            "+----+------+------+----+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_concat_dfs() -> Result<()> {
    let ctx = SessionContext::new();
    let df1 = get_df1()?;
    let df2 = get_df1()?;

    let res = concat_dfs(&ctx, vec![df1, df2]).await?;

    assert_eq!(res.schema().fields().len(), 3); // columns count
    assert_eq!(res.clone().count().await?, 6); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | name | data |",
            "+----+------+------+",
            "| 1  | foo  | 42   |",
            "| 1  | foo  | 42   |",
            "| 2  | bar  | 43   |",
            "| 2  | bar  | 43   |",
            "| 3  | baz  | 44   |",
            "| 3  | baz  | 44   |",
            "+----+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_int_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_df2()?;

    let data = vec![42, 43, 44];
    let res = add_int_col_to_df(&ctx, df, data, "data").await?;

    assert_eq!(res.schema().fields().len(), 3); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | name | data |",
            "+----+------+------+",
            "| 1  | foo  | 42   |",
            "| 2  | bar  | 43   |",
            "| 3  | baz  | 44   |",
            "+----+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_str_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_df3()?;

    let data = vec!["foo", "bar", "baz"];
    let res = add_str_col_to_df(&ctx, df, data, "name").await?;

    assert_eq!(res.schema().fields().len(), 3); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
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
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_any_num_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_df3()?;

    let data = vec![1, 2, 3];
    let data_col = Int32Array::from(data);
    let df = add_any_num_col_to_df(&ctx, df, data_col, "col1").await?;

    let data = vec![1.1, 1.2, 1.3];
    let data_col = Float64Array::from(data);
    let res = add_any_num_col_to_df(&ctx, df, data_col, "col2").await?;

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+------+",
            "| id | data | col1 | col2 |",
            "+----+------+------+------+",
            "| 1  | 42   | 1    | 1.1  |",
            "| 2  | 43   | 2    | 1.2  |",
            "| 3  | 44   | 3    | 1.3  |",
            "+----+------+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_any_str_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_df3()?;

    let data = vec!["foo", "bar", "baz"];
    let data_col = StringArray::from(data);
    let df = add_any_str_col_to_df(&ctx, df, data_col, "col1").await?;

    let data = vec!["foo", "bar", "baz"];
    let data_col = LargeStringArray::from(data);
    let res = add_any_str_col_to_df(&ctx, df, data_col, "col2").await?;

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+------+",
            "| id | data | col1 | col2 |",
            "+----+------+------+------+",
            "| 1  | 42   | foo  | foo  |",
            "| 2  | 43   | bar  | bar  |",
            "| 3  | 44   | baz  | baz  |",
            "+----+------+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_df3()?;

    let col1 = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));
    let col2 = Arc::new(Float64Array::from(vec![42.0, 43.0, 44.0]));
    let df = add_col_to_df(&ctx, df, col1, "col1").await?;
    let res = add_col_to_df(&ctx, df, col2, "col2").await?;

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+------+",
            "| id | data | col1 | col2 |",
            "+----+------+------+------+",
            "| 1  | 42   | foo  | 42.0 |",
            "| 2  | 43   | bar  | 43.0 |",
            "| 3  | 44   | baz  | 44.0 |",
            "+----+------+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_col_arr_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_df3()?;

    let col1 = StringArray::from(vec!["foo", "bar", "baz"]);
    let col2 = Float64Array::from(vec![42.0, 43.0, 44.0]);
    let df = add_col_arr_to_df(&ctx, df, &col1, "col1").await?;
    let res = add_col_arr_to_df(&ctx, df, &col2, "col2").await?;

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+------+",
            "| id | data | col1 | col2 |",
            "+----+------+------+------+",
            "| 1  | 42   | foo  | 42.0 |",
            "| 2  | 43   | bar  | 43.0 |",
            "| 3  | 44   | baz  | 44.0 |",
            "+----+------+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}
