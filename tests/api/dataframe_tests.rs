use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, ListArray, RecordBatch, StringArray,
    StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::assert_batches_eq;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use datafusion_example::utils::helpers::{get_empty_df, select_all_exclude};
use serde_json::{Map, Value};
use tempfile::tempdir;

use datafusion_example::utils::dataframe::*;

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
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;

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
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;

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
        &rows.clone().collect().await?
    );

    let batches = rows.collect().await?;
    let mut metadata = Vec::new();
    for batch in batches.iter() {
        let col_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");

        for i in 0..col_array.len() {
            if col_array.is_valid(i) {
                metadata.push(Some(col_array.value(i)));
            } else {
                metadata.push(None);
            }
        }
    }
    assert_eq!(metadata[0], Some(r#"{"data":42,"name":"foo"}"#));
    assert_eq!(metadata[1], Some(r#"{"data":43,"name":"bar"}"#));
    assert_eq!(metadata[2], Some(r#"{"data":44,"name":"baz"}"#));
    Ok(())
}

#[tokio::test]
async fn test_cols_to_struct() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;

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
async fn test_concat_dfs() -> Result<()> {
    let ctx = SessionContext::new();
    let df1 = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;
    let df2 = df1.clone();

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
async fn test_concat_df_batches() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;
    let batch = concat_df_batches(df).await?;
    ctx.register_batch("t", batch)?;
    let res = ctx.sql("select * from t order by id").await?;
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
        &res.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_concat_arays() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;
    let schema_ref = Arc::new(df.schema().as_arrow().clone());
    let arrays: Vec<ArrayRef> = concat_arrays(df).await?;
    let batch = RecordBatch::try_new(schema_ref.clone(), arrays)?;
    ctx.register_batch("t", batch)?;
    let res = ctx.sql("select * from t order by id").await?;
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
        &res.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_select_all_exclude() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;
    let res = select_all_exclude(df, &["data"])?;

    assert_eq!(res.schema().fields().len(), 2); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
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
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_df_sql() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;
    let sql = r#"id > 2 and data > 43 and name in ('foo', 'bar', 'baz')"#;
    let res = df_sql(df, sql)?;

    assert_eq!(res.schema().fields().len(), 3);
    assert_eq!(res.clone().count().await?, 1);

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | name | data |",
            "+----+------+------+",
            "| 3  | baz  | 44   |",
            "+----+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_is_empty() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;
    assert_eq!(is_empty(df).await?, false);

    let df = ctx.read_empty()?;
    assert_eq!(is_empty(df).await?, false);

    let schema = Schema::empty();
    let batch = RecordBatch::new_empty(Arc::new(schema));
    let df = ctx.read_batch(batch)?;
    assert_eq!(is_empty(df).await?, true);

    Ok(())
}

#[tokio::test]
async fn test_get_empty_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_empty_df(&ctx)?;
    assert_eq!(df.schema().fields().len(), 0);
    assert_eq!(df.clone().count().await?, 0);

    Ok(())
}

#[tokio::test]
async fn test_df_to_table() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;
    df_to_table(&ctx, df, "t").await?;
    let res = ctx.sql("select * from t order by id").await?;
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
        &res.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_df_plan_to_table() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;
    df_plan_to_table(&ctx, df.logical_plan().clone(), "t")?;
    let res = ctx.sql("select * from t order by id").await?;
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
        &res.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_write_df_to_file() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;
    let dir = tempdir()?;
    let file_path = dir.path().join("foo.parquet");
    write_df_to_file(df, file_path.to_str().unwrap()).await?;
    let res = ctx
        .read_parquet(file_path.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
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
async fn test_read_file_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;
    let dir = tempdir()?;
    let file_path = dir.path().join("foo.parquet");
    df.write_parquet(
        file_path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        None,
    )
    .await?;
    let res = read_file_to_df(&ctx, file_path.to_str().unwrap()).await?;
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
async fn test_add_column_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "data" => [42, 43, 44]
    )?;

    let col0: ArrayRef = Arc::new(Int32Array::from(vec![10, 100, 1000])); // add int array
    let col1: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"])); // add str array
    let col2: ArrayRef = Arc::new(Float64Array::from(vec![42.0, 43.0, 44.0])); // add float array
    let col3: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])); // add bools array
    let data1: Vec<Option<Vec<Option<i32>>>> = vec![None; 3]; // add list array of nulls
    let col4: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data1));
    let data2 = vec![
        // add list array
        Some(vec![Some(0), Some(1), Some(2)]),
        None,
        Some(vec![Some(3), None, Some(4)]),
    ];
    let col5: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data2));
    let col6: ArrayRef = Arc::new(StructArray::from(vec![
        // add struct array
        (
            Arc::new(Field::new("a", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef,
        ),
    ]));
    let df = add_column_to_df(&ctx, df, col0, "col0").await?;
    let df = add_column_to_df(&ctx, df, col1, "col1").await?;
    let df = add_column_to_df(&ctx, df, col2, "col2").await?;
    let df = add_column_to_df(&ctx, df, col3, "col3").await?;
    let df = add_column_to_df(&ctx, df, col4, "col4").await?;
    let df = add_column_to_df(&ctx, df, col5, "col5").await?;
    let res = add_column_to_df(&ctx, df, col6, "col6").await?;

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+------+------+-------+------+-----------+-----------------+",
            "| id | data | col0 | col1 | col2 | col3  | col4 | col5      | col6            |",
            "+----+------+------+------+------+-------+------+-----------+-----------------+",
            "| 1  | 42   | 10   | foo  | 42.0 | true  |      | [0, 1, 2] | {a: foo, b: 42} |",
            "| 2  | 43   | 100  | bar  | 43.0 |       |      |           | {a: bar, b: 43} |",
            "| 3  | 44   | 1000 | baz  | 44.0 | false |      | [3, , 4]  | {a: baz, b: 44} |",
            "+----+------+------+------+------+-------+------+-----------+-----------------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_df_to_json_bytes() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"]
    )?;
    let json_data = df_to_json_bytes(df).await?;
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;

    assert_eq!(
        serde_json::Value::Object(json_rows[0].clone()),
        serde_json::json!({"id": 1, "name": "foo"}),
    );

    assert_eq!(
        serde_json::Value::Object(json_rows[1].clone()),
        serde_json::json!({"id": 2, "name": "bar"}),
    );

    assert_eq!(
        serde_json::Value::Object(json_rows[2].clone()),
        serde_json::json!({"id": 3, "name": "baz"}),
    );

    Ok(())
}

#[tokio::test]
async fn test_join_dfs() -> Result<()> {
    let df1 = dataframe!(
        "id" => [1, 2, 3],
        "pk" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42., 43., 44.]
    )?;
    let df2 = dataframe!(
        "id" => [1, 2, 3],
        "pk" => [1, 2, 3],
        "value" => [true, true, false]
    )?;
    let df3 = dataframe!(
        "id" => [1, 2, 3],
        "pk" => [1, 2, 3],
        "nums" => [42, 42, 42]
    )?;
    let df4 = dataframe!(
        "id" => [1, 2, 3],
        "pk" => [1, 2, 3],
        "store" => [1, 10, 100]
    )?;
    let res = join_dfs(vec![df1, df2, df3, df4], &["id", "pk"])?;
    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+----+------+------+-------+------+-------+",
            "| id | pk | name | data | value | nums | store |",
            "+----+----+------+------+-------+------+-------+",
            "| 1  | 1  | foo  | 42.0 | true  | 42   | 1     |",
            "| 2  | 2  | bar  | 43.0 | true  | 42   | 10    |",
            "| 3  | 3  | baz  | 44.0 | false | 42   | 100   |",
            "+----+----+------+------+-------+------+-------+",
        ],
        &rows.collect().await?
    );
    Ok(())
}
