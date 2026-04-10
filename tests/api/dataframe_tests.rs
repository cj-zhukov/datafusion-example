use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, ListArray, RecordBatch, StringArray,
    StructArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::assert_batches_eq;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use serde_json::{Map, Value};
use tempfile::tempdir;

use datafusion_example::utils::dataframe::*;
use datafusion_example::utils::datasets::ExampleDataset;
use datafusion_example::utils::helpers::{get_empty_df, select_all_exclude};

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
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;

    let arrays = concat_arrays(df).await?;
    assert_eq!(arrays.len(), 3);

    let cars = arrays.get(0).unwrap().as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(cars.value(0), "red");
    assert_eq!(cars.value(1), "red");
    assert_eq!(cars.value(2), "red");
    assert_eq!(cars.value(2), "red");
    assert_eq!(cars.value(2), "red");
    
    let speed_all= arrays
        .get(1)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(speed_all.values(), &[20.0, 20.3, 21.4, 21.5, 19.0, 18.0, 17.0, 7.0, 7.1, 7.2, 3.0, 1.0, 0.0, 10.0, 10.3, 10.4, 10.5, 11.0, 12.0, 14.0, 15.0, 15.1, 15.2, 8.0, 2.0]);

    let time_all = arrays
        .get(2)
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(time_all.values(), &[829310703000000000, 829310704000000000, 829310705000000000, 829310706000000000, 829310707000000000, 829310708000000000, 829310709000000000, 829310710000000000, 829310711000000000, 829310712000000000, 829310713000000000, 829310714000000000, 829310715000000000, 829310703000000000, 829310704000000000, 829310705000000000, 829310706000000000, 829310707000000000, 829310708000000000, 829310709000000000, 829310710000000000, 829310711000000000, 829310712000000000, 829310713000000000, 829310714000000000]);

    Ok(())
}

#[tokio::test]
async fn test_cols_to_json() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;

    let ctx = SessionContext::new();
    let res = df_cols_to_json(&ctx, df, &["car", "speed"], "metadata").await?;

    assert_eq!(res.schema().fields().len(), 2); // columns count
    assert_eq!(res.clone().count().await?, 25); // rows count

    let rows = res.sort(vec![col("time").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+---------------------+------------------------------+",
            "| time                | metadata                     |",
            "+---------------------+------------------------------+",
            "| 1996-04-12T12:05:03 | {\"car\":\"red\",\"speed\":20.0}   |",
            "| 1996-04-12T12:05:03 | {\"car\":\"green\",\"speed\":10.0} |",
            "| 1996-04-12T12:05:04 | {\"car\":\"red\",\"speed\":20.3}   |",
            "| 1996-04-12T12:05:04 | {\"car\":\"green\",\"speed\":10.3} |",
            "| 1996-04-12T12:05:05 | {\"car\":\"red\",\"speed\":21.4}   |",
            "| 1996-04-12T12:05:05 | {\"car\":\"green\",\"speed\":10.4} |",
            "| 1996-04-12T12:05:06 | {\"car\":\"red\",\"speed\":21.5}   |",
            "| 1996-04-12T12:05:06 | {\"car\":\"green\",\"speed\":10.5} |",
            "| 1996-04-12T12:05:07 | {\"car\":\"red\",\"speed\":19.0}   |",
            "| 1996-04-12T12:05:07 | {\"car\":\"green\",\"speed\":11.0} |",
            "| 1996-04-12T12:05:08 | {\"car\":\"red\",\"speed\":18.0}   |",
            "| 1996-04-12T12:05:08 | {\"car\":\"green\",\"speed\":12.0} |",
            "| 1996-04-12T12:05:09 | {\"car\":\"red\",\"speed\":17.0}   |",
            "| 1996-04-12T12:05:09 | {\"car\":\"green\",\"speed\":14.0} |",
            "| 1996-04-12T12:05:10 | {\"car\":\"red\",\"speed\":7.0}    |",
            "| 1996-04-12T12:05:10 | {\"car\":\"green\",\"speed\":15.0} |",
            "| 1996-04-12T12:05:11 | {\"car\":\"red\",\"speed\":7.1}    |",
            "| 1996-04-12T12:05:11 | {\"car\":\"green\",\"speed\":15.1} |",
            "| 1996-04-12T12:05:12 | {\"car\":\"red\",\"speed\":7.2}    |",
            "| 1996-04-12T12:05:12 | {\"car\":\"green\",\"speed\":15.2} |",
            "| 1996-04-12T12:05:13 | {\"car\":\"red\",\"speed\":3.0}    |",
            "| 1996-04-12T12:05:13 | {\"car\":\"green\",\"speed\":8.0}  |",
            "| 1996-04-12T12:05:14 | {\"car\":\"red\",\"speed\":1.0}    |",
            "| 1996-04-12T12:05:14 | {\"car\":\"green\",\"speed\":2.0}  |",
            "| 1996-04-12T12:05:15 | {\"car\":\"red\",\"speed\":0.0}    |",
            "+---------------------+------------------------------+",
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
    assert_eq!(metadata[0], Some("{\"car\":\"red\",\"speed\":20.0}")); 
    assert_eq!(metadata[1], Some("{\"car\":\"green\",\"speed\":10.0}")); 
    assert_eq!(metadata[2], Some("{\"car\":\"red\",\"speed\":20.3}"));
    assert_eq!(metadata[22], Some("{\"car\":\"red\",\"speed\":1.0}"));
    assert_eq!(metadata[23], Some("{\"car\":\"green\",\"speed\":2.0}"));
    assert_eq!(metadata[24], Some("{\"car\":\"red\",\"speed\":0.0}"));
    Ok(())
}

#[tokio::test]
async fn test_cols_to_struct() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;

    let ctx = SessionContext::new();
    let res = df_cols_to_struct(&ctx, df, &["car", "speed"], "metadata").await?;

    assert_eq!(res.schema().fields().len(), 2); // columns count
    assert_eq!(res.clone().count().await?, 25); // rows count

    let rows = res.sort(vec![col("time").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+---------------------+---------------------------+",
            "| time                | metadata                  |",
            "+---------------------+---------------------------+",
            "| 1996-04-12T12:05:03 | {car: red, speed: 20.0}   |",
            "| 1996-04-12T12:05:03 | {car: green, speed: 10.0} |",
            "| 1996-04-12T12:05:04 | {car: red, speed: 20.3}   |",
            "| 1996-04-12T12:05:04 | {car: green, speed: 10.3} |",
            "| 1996-04-12T12:05:05 | {car: red, speed: 21.4}   |",
            "| 1996-04-12T12:05:05 | {car: green, speed: 10.4} |",
            "| 1996-04-12T12:05:06 | {car: red, speed: 21.5}   |",
            "| 1996-04-12T12:05:06 | {car: green, speed: 10.5} |",
            "| 1996-04-12T12:05:07 | {car: red, speed: 19.0}   |",
            "| 1996-04-12T12:05:07 | {car: green, speed: 11.0} |",
            "| 1996-04-12T12:05:08 | {car: red, speed: 18.0}   |",
            "| 1996-04-12T12:05:08 | {car: green, speed: 12.0} |",
            "| 1996-04-12T12:05:09 | {car: red, speed: 17.0}   |",
            "| 1996-04-12T12:05:09 | {car: green, speed: 14.0} |",
            "| 1996-04-12T12:05:10 | {car: red, speed: 7.0}    |",
            "| 1996-04-12T12:05:10 | {car: green, speed: 15.0} |",
            "| 1996-04-12T12:05:11 | {car: red, speed: 7.1}    |",
            "| 1996-04-12T12:05:11 | {car: green, speed: 15.1} |",
            "| 1996-04-12T12:05:12 | {car: red, speed: 7.2}    |",
            "| 1996-04-12T12:05:12 | {car: green, speed: 15.2} |",
            "| 1996-04-12T12:05:13 | {car: red, speed: 3.0}    |",
            "| 1996-04-12T12:05:13 | {car: green, speed: 8.0}  |",
            "| 1996-04-12T12:05:14 | {car: red, speed: 1.0}    |",
            "| 1996-04-12T12:05:14 | {car: green, speed: 2.0}  |",
            "| 1996-04-12T12:05:15 | {car: red, speed: 0.0}    |",
            "+---------------------+---------------------------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_concat_dfs() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df1 = cars.dataframe(&ctx).await?;
    let df2 = df1.clone();

    let res = concat_dfs(&ctx, vec![df1, df2]).await?;

    assert_eq!(res.schema().fields().len(), 3); // columns count
    assert_eq!(res.clone().count().await?, 50); // rows count

    let rows = res.sort(vec![col("speed").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+",
            "| car   | speed | time                |",
            "+-------+-------+---------------------+",
            "| red   | 0.0   | 1996-04-12T12:05:15 |",
            "| red   | 0.0   | 1996-04-12T12:05:15 |",
            "| red   | 1.0   | 1996-04-12T12:05:14 |",
            "| red   | 1.0   | 1996-04-12T12:05:14 |",
            "| green | 2.0   | 1996-04-12T12:05:14 |",
            "| green | 2.0   | 1996-04-12T12:05:14 |",
            "| red   | 3.0   | 1996-04-12T12:05:13 |",
            "| red   | 3.0   | 1996-04-12T12:05:13 |",
            "| red   | 7.0   | 1996-04-12T12:05:10 |",
            "| red   | 7.0   | 1996-04-12T12:05:10 |",
            "| red   | 7.1   | 1996-04-12T12:05:11 |",
            "| red   | 7.1   | 1996-04-12T12:05:11 |",
            "| red   | 7.2   | 1996-04-12T12:05:12 |",
            "| red   | 7.2   | 1996-04-12T12:05:12 |",
            "| green | 8.0   | 1996-04-12T12:05:13 |",
            "| green | 8.0   | 1996-04-12T12:05:13 |",
            "| green | 10.0  | 1996-04-12T12:05:03 |",
            "| green | 10.0  | 1996-04-12T12:05:03 |",
            "| green | 10.3  | 1996-04-12T12:05:04 |",
            "| green | 10.3  | 1996-04-12T12:05:04 |",
            "| green | 10.4  | 1996-04-12T12:05:05 |",
            "| green | 10.4  | 1996-04-12T12:05:05 |",
            "| green | 10.5  | 1996-04-12T12:05:06 |",
            "| green | 10.5  | 1996-04-12T12:05:06 |",
            "| green | 11.0  | 1996-04-12T12:05:07 |",
            "| green | 11.0  | 1996-04-12T12:05:07 |",
            "| green | 12.0  | 1996-04-12T12:05:08 |",
            "| green | 12.0  | 1996-04-12T12:05:08 |",
            "| green | 14.0  | 1996-04-12T12:05:09 |",
            "| green | 14.0  | 1996-04-12T12:05:09 |",
            "| green | 15.0  | 1996-04-12T12:05:10 |",
            "| green | 15.0  | 1996-04-12T12:05:10 |",
            "| green | 15.1  | 1996-04-12T12:05:11 |",
            "| green | 15.1  | 1996-04-12T12:05:11 |",
            "| green | 15.2  | 1996-04-12T12:05:12 |",
            "| green | 15.2  | 1996-04-12T12:05:12 |",
            "| red   | 17.0  | 1996-04-12T12:05:09 |",
            "| red   | 17.0  | 1996-04-12T12:05:09 |",
            "| red   | 18.0  | 1996-04-12T12:05:08 |",
            "| red   | 18.0  | 1996-04-12T12:05:08 |",
            "| red   | 19.0  | 1996-04-12T12:05:07 |",
            "| red   | 19.0  | 1996-04-12T12:05:07 |",
            "| red   | 20.0  | 1996-04-12T12:05:03 |",
            "| red   | 20.0  | 1996-04-12T12:05:03 |",
            "| red   | 20.3  | 1996-04-12T12:05:04 |",
            "| red   | 20.3  | 1996-04-12T12:05:04 |",
            "| red   | 21.4  | 1996-04-12T12:05:05 |",
            "| red   | 21.4  | 1996-04-12T12:05:05 |",
            "| red   | 21.5  | 1996-04-12T12:05:06 |",
            "| red   | 21.5  | 1996-04-12T12:05:06 |",
            "+-------+-------+---------------------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_concat_df_batches() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;

    let batch = concat_df_batches(df).await?;
    ctx.register_batch("cars", batch)?;
    let res = ctx.sql("select * from cars order by speed").await?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+",
            "| car   | speed | time                |",
            "+-------+-------+---------------------+",
            "| red   | 0.0   | 1996-04-12T12:05:15 |",
            "| red   | 1.0   | 1996-04-12T12:05:14 |",
            "| green | 2.0   | 1996-04-12T12:05:14 |",
            "| red   | 3.0   | 1996-04-12T12:05:13 |",
            "| red   | 7.0   | 1996-04-12T12:05:10 |",
            "| red   | 7.1   | 1996-04-12T12:05:11 |",
            "| red   | 7.2   | 1996-04-12T12:05:12 |",
            "| green | 8.0   | 1996-04-12T12:05:13 |",
            "| green | 10.0  | 1996-04-12T12:05:03 |",
            "| green | 10.3  | 1996-04-12T12:05:04 |",
            "| green | 10.4  | 1996-04-12T12:05:05 |",
            "| green | 10.5  | 1996-04-12T12:05:06 |",
            "| green | 11.0  | 1996-04-12T12:05:07 |",
            "| green | 12.0  | 1996-04-12T12:05:08 |",
            "| green | 14.0  | 1996-04-12T12:05:09 |",
            "| green | 15.0  | 1996-04-12T12:05:10 |",
            "| green | 15.1  | 1996-04-12T12:05:11 |",
            "| green | 15.2  | 1996-04-12T12:05:12 |",
            "| red   | 17.0  | 1996-04-12T12:05:09 |",
            "| red   | 18.0  | 1996-04-12T12:05:08 |",
            "| red   | 19.0  | 1996-04-12T12:05:07 |",
            "| red   | 20.0  | 1996-04-12T12:05:03 |",
            "| red   | 20.3  | 1996-04-12T12:05:04 |",
            "| red   | 21.4  | 1996-04-12T12:05:05 |",
            "| red   | 21.5  | 1996-04-12T12:05:06 |",
            "+-------+-------+---------------------+",
        ],
        &res.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_concat_arays() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;
    let schema_ref = Arc::new(df.schema().as_arrow().clone());
    let arrays: Vec<ArrayRef> = concat_arrays(df).await?;
    let batch = RecordBatch::try_new(schema_ref.clone(), arrays)?;
    ctx.register_batch("cars", batch)?;
    let res = ctx.sql("select * from cars order by speed").await?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+",
            "| car   | speed | time                |",
            "+-------+-------+---------------------+",
            "| red   | 0.0   | 1996-04-12T12:05:15 |",
            "| red   | 1.0   | 1996-04-12T12:05:14 |",
            "| green | 2.0   | 1996-04-12T12:05:14 |",
            "| red   | 3.0   | 1996-04-12T12:05:13 |",
            "| red   | 7.0   | 1996-04-12T12:05:10 |",
            "| red   | 7.1   | 1996-04-12T12:05:11 |",
            "| red   | 7.2   | 1996-04-12T12:05:12 |",
            "| green | 8.0   | 1996-04-12T12:05:13 |",
            "| green | 10.0  | 1996-04-12T12:05:03 |",
            "| green | 10.3  | 1996-04-12T12:05:04 |",
            "| green | 10.4  | 1996-04-12T12:05:05 |",
            "| green | 10.5  | 1996-04-12T12:05:06 |",
            "| green | 11.0  | 1996-04-12T12:05:07 |",
            "| green | 12.0  | 1996-04-12T12:05:08 |",
            "| green | 14.0  | 1996-04-12T12:05:09 |",
            "| green | 15.0  | 1996-04-12T12:05:10 |",
            "| green | 15.1  | 1996-04-12T12:05:11 |",
            "| green | 15.2  | 1996-04-12T12:05:12 |",
            "| red   | 17.0  | 1996-04-12T12:05:09 |",
            "| red   | 18.0  | 1996-04-12T12:05:08 |",
            "| red   | 19.0  | 1996-04-12T12:05:07 |",
            "| red   | 20.0  | 1996-04-12T12:05:03 |",
            "| red   | 20.3  | 1996-04-12T12:05:04 |",
            "| red   | 21.4  | 1996-04-12T12:05:05 |",
            "| red   | 21.5  | 1996-04-12T12:05:06 |",
            "+-------+-------+---------------------+",
        ],
        &res.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_select_all_exclude() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;
    let res = select_all_exclude(df, &["time"])?;

    assert_eq!(res.schema().fields().len(), 2); // columns count
    assert_eq!(res.clone().count().await?, 25); // rows count

    let rows = res.sort(vec![col("speed").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-------+-------+",
            "| car   | speed |",
            "+-------+-------+",
            "| red   | 0.0   |",
            "| red   | 1.0   |",
            "| green | 2.0   |",
            "| red   | 3.0   |",
            "| red   | 7.0   |",
            "| red   | 7.1   |",
            "| red   | 7.2   |",
            "| green | 8.0   |",
            "| green | 10.0  |",
            "| green | 10.3  |",
            "| green | 10.4  |",
            "| green | 10.5  |",
            "| green | 11.0  |",
            "| green | 12.0  |",
            "| green | 14.0  |",
            "| green | 15.0  |",
            "| green | 15.1  |",
            "| green | 15.2  |",
            "| red   | 17.0  |",
            "| red   | 18.0  |",
            "| red   | 19.0  |",
            "| red   | 20.0  |",
            "| red   | 20.3  |",
            "| red   | 21.4  |",
            "| red   | 21.5  |",
            "+-------+-------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_df_sql() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;
    let sql = r#"speed > 5.0 and car in ('red', 'green')"#;
    let res = df_sql(df, sql)?;

    assert_eq!(res.schema().fields().len(), 3);
    assert_eq!(res.clone().count().await?, 21);

    let rows = res.sort(vec![col("speed").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+",
            "| car   | speed | time                |",
            "+-------+-------+---------------------+",
            "| red   | 7.0   | 1996-04-12T12:05:10 |",
            "| red   | 7.1   | 1996-04-12T12:05:11 |",
            "| red   | 7.2   | 1996-04-12T12:05:12 |",
            "| green | 8.0   | 1996-04-12T12:05:13 |",
            "| green | 10.0  | 1996-04-12T12:05:03 |",
            "| green | 10.3  | 1996-04-12T12:05:04 |",
            "| green | 10.4  | 1996-04-12T12:05:05 |",
            "| green | 10.5  | 1996-04-12T12:05:06 |",
            "| green | 11.0  | 1996-04-12T12:05:07 |",
            "| green | 12.0  | 1996-04-12T12:05:08 |",
            "| green | 14.0  | 1996-04-12T12:05:09 |",
            "| green | 15.0  | 1996-04-12T12:05:10 |",
            "| green | 15.1  | 1996-04-12T12:05:11 |",
            "| green | 15.2  | 1996-04-12T12:05:12 |",
            "| red   | 17.0  | 1996-04-12T12:05:09 |",
            "| red   | 18.0  | 1996-04-12T12:05:08 |",
            "| red   | 19.0  | 1996-04-12T12:05:07 |",
            "| red   | 20.0  | 1996-04-12T12:05:03 |",
            "| red   | 20.3  | 1996-04-12T12:05:04 |",
            "| red   | 21.4  | 1996-04-12T12:05:05 |",
            "| red   | 21.5  | 1996-04-12T12:05:06 |",
            "+-------+-------+---------------------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_is_empty() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;
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
async fn test_register_materialized_df() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;
    register_materialized_df(&ctx, df, "cars").await?;
    let res = ctx.sql("select * from cars order by speed").await?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+",
            "| car   | speed | time                |",
            "+-------+-------+---------------------+",
            "| red   | 0.0   | 1996-04-12T12:05:15 |",
            "| red   | 1.0   | 1996-04-12T12:05:14 |",
            "| green | 2.0   | 1996-04-12T12:05:14 |",
            "| red   | 3.0   | 1996-04-12T12:05:13 |",
            "| red   | 7.0   | 1996-04-12T12:05:10 |",
            "| red   | 7.1   | 1996-04-12T12:05:11 |",
            "| red   | 7.2   | 1996-04-12T12:05:12 |",
            "| green | 8.0   | 1996-04-12T12:05:13 |",
            "| green | 10.0  | 1996-04-12T12:05:03 |",
            "| green | 10.3  | 1996-04-12T12:05:04 |",
            "| green | 10.4  | 1996-04-12T12:05:05 |",
            "| green | 10.5  | 1996-04-12T12:05:06 |",
            "| green | 11.0  | 1996-04-12T12:05:07 |",
            "| green | 12.0  | 1996-04-12T12:05:08 |",
            "| green | 14.0  | 1996-04-12T12:05:09 |",
            "| green | 15.0  | 1996-04-12T12:05:10 |",
            "| green | 15.1  | 1996-04-12T12:05:11 |",
            "| green | 15.2  | 1996-04-12T12:05:12 |",
            "| red   | 17.0  | 1996-04-12T12:05:09 |",
            "| red   | 18.0  | 1996-04-12T12:05:08 |",
            "| red   | 19.0  | 1996-04-12T12:05:07 |",
            "| red   | 20.0  | 1996-04-12T12:05:03 |",
            "| red   | 20.3  | 1996-04-12T12:05:04 |",
            "| red   | 21.4  | 1996-04-12T12:05:05 |",
            "| red   | 21.5  | 1996-04-12T12:05:06 |",
            "+-------+-------+---------------------+",
        ],
        &res.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_register_df_view() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;
    register_df_view(&ctx, &df,"cars")?;
    let res = ctx.sql("select * from cars order by speed").await?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+",
            "| car   | speed | time                |",
            "+-------+-------+---------------------+",
            "| red   | 0.0   | 1996-04-12T12:05:15 |",
            "| red   | 1.0   | 1996-04-12T12:05:14 |",
            "| green | 2.0   | 1996-04-12T12:05:14 |",
            "| red   | 3.0   | 1996-04-12T12:05:13 |",
            "| red   | 7.0   | 1996-04-12T12:05:10 |",
            "| red   | 7.1   | 1996-04-12T12:05:11 |",
            "| red   | 7.2   | 1996-04-12T12:05:12 |",
            "| green | 8.0   | 1996-04-12T12:05:13 |",
            "| green | 10.0  | 1996-04-12T12:05:03 |",
            "| green | 10.3  | 1996-04-12T12:05:04 |",
            "| green | 10.4  | 1996-04-12T12:05:05 |",
            "| green | 10.5  | 1996-04-12T12:05:06 |",
            "| green | 11.0  | 1996-04-12T12:05:07 |",
            "| green | 12.0  | 1996-04-12T12:05:08 |",
            "| green | 14.0  | 1996-04-12T12:05:09 |",
            "| green | 15.0  | 1996-04-12T12:05:10 |",
            "| green | 15.1  | 1996-04-12T12:05:11 |",
            "| green | 15.2  | 1996-04-12T12:05:12 |",
            "| red   | 17.0  | 1996-04-12T12:05:09 |",
            "| red   | 18.0  | 1996-04-12T12:05:08 |",
            "| red   | 19.0  | 1996-04-12T12:05:07 |",
            "| red   | 20.0  | 1996-04-12T12:05:03 |",
            "| red   | 20.3  | 1996-04-12T12:05:04 |",
            "| red   | 21.4  | 1996-04-12T12:05:05 |",
            "| red   | 21.5  | 1996-04-12T12:05:06 |",
            "+-------+-------+---------------------+",
        ],
        &res.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_write_df_to_file() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;
    let dir = tempdir()?;
    let file_path = dir.path().join("cars.parquet");
    write_df_to_file(df, file_path.to_str().unwrap()).await?;
    let res = ctx
        .read_parquet(file_path.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    let rows = res.sort(vec![col("speed").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+",
            "| car   | speed | time                |",
            "+-------+-------+---------------------+",
            "| red   | 0.0   | 1996-04-12T12:05:15 |",
            "| red   | 1.0   | 1996-04-12T12:05:14 |",
            "| green | 2.0   | 1996-04-12T12:05:14 |",
            "| red   | 3.0   | 1996-04-12T12:05:13 |",
            "| red   | 7.0   | 1996-04-12T12:05:10 |",
            "| red   | 7.1   | 1996-04-12T12:05:11 |",
            "| red   | 7.2   | 1996-04-12T12:05:12 |",
            "| green | 8.0   | 1996-04-12T12:05:13 |",
            "| green | 10.0  | 1996-04-12T12:05:03 |",
            "| green | 10.3  | 1996-04-12T12:05:04 |",
            "| green | 10.4  | 1996-04-12T12:05:05 |",
            "| green | 10.5  | 1996-04-12T12:05:06 |",
            "| green | 11.0  | 1996-04-12T12:05:07 |",
            "| green | 12.0  | 1996-04-12T12:05:08 |",
            "| green | 14.0  | 1996-04-12T12:05:09 |",
            "| green | 15.0  | 1996-04-12T12:05:10 |",
            "| green | 15.1  | 1996-04-12T12:05:11 |",
            "| green | 15.2  | 1996-04-12T12:05:12 |",
            "| red   | 17.0  | 1996-04-12T12:05:09 |",
            "| red   | 18.0  | 1996-04-12T12:05:08 |",
            "| red   | 19.0  | 1996-04-12T12:05:07 |",
            "| red   | 20.0  | 1996-04-12T12:05:03 |",
            "| red   | 20.3  | 1996-04-12T12:05:04 |",
            "| red   | 21.4  | 1996-04-12T12:05:05 |",
            "| red   | 21.5  | 1996-04-12T12:05:06 |",
            "+-------+-------+---------------------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_read_file_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;
    let dir = tempdir()?;
    let file_path = dir.path().join("cars.parquet");
    df.write_parquet(
        file_path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        None,
    )
    .await?;
    let res = read_file_to_df(&ctx, file_path.to_str().unwrap()).await?;
    let rows = res.sort(vec![col("speed").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+",
            "| car   | speed | time                |",
            "+-------+-------+---------------------+",
            "| red   | 0.0   | 1996-04-12T12:05:15 |",
            "| red   | 1.0   | 1996-04-12T12:05:14 |",
            "| green | 2.0   | 1996-04-12T12:05:14 |",
            "| red   | 3.0   | 1996-04-12T12:05:13 |",
            "| red   | 7.0   | 1996-04-12T12:05:10 |",
            "| red   | 7.1   | 1996-04-12T12:05:11 |",
            "| red   | 7.2   | 1996-04-12T12:05:12 |",
            "| green | 8.0   | 1996-04-12T12:05:13 |",
            "| green | 10.0  | 1996-04-12T12:05:03 |",
            "| green | 10.3  | 1996-04-12T12:05:04 |",
            "| green | 10.4  | 1996-04-12T12:05:05 |",
            "| green | 10.5  | 1996-04-12T12:05:06 |",
            "| green | 11.0  | 1996-04-12T12:05:07 |",
            "| green | 12.0  | 1996-04-12T12:05:08 |",
            "| green | 14.0  | 1996-04-12T12:05:09 |",
            "| green | 15.0  | 1996-04-12T12:05:10 |",
            "| green | 15.1  | 1996-04-12T12:05:11 |",
            "| green | 15.2  | 1996-04-12T12:05:12 |",
            "| red   | 17.0  | 1996-04-12T12:05:09 |",
            "| red   | 18.0  | 1996-04-12T12:05:08 |",
            "| red   | 19.0  | 1996-04-12T12:05:07 |",
            "| red   | 20.0  | 1996-04-12T12:05:03 |",
            "| red   | 20.3  | 1996-04-12T12:05:04 |",
            "| red   | 21.4  | 1996-04-12T12:05:05 |",
            "| red   | 21.5  | 1996-04-12T12:05:06 |",
            "+-------+-------+---------------------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_column_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx)
        .await?
        .limit(0, Some(3))?
        .sort(vec![col("speed").sort(true, true)])?;

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

    let rows = res.sort(vec![col("speed").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-----+-------+---------------------+------+------+------+-------+------+-----------+-----------------+",
            "| car | speed | time                | col0 | col1 | col2 | col3  | col4 | col5      | col6            |",
            "+-----+-------+---------------------+------+------+------+-------+------+-----------+-----------------+",
            "| red | 20.0  | 1996-04-12T12:05:03 | 10   | foo  | 42.0 | true  |      | [0, 1, 2] | {a: foo, b: 42} |",
            "| red | 20.3  | 1996-04-12T12:05:04 | 100  | bar  | 43.0 |       |      |           | {a: bar, b: 43} |",
            "| red | 21.4  | 1996-04-12T12:05:05 | 1000 | baz  | 44.0 | false |      | [3, , 4]  | {a: baz, b: 44} |",
            "+-----+-------+---------------------+------+------+------+-------+------+-----------+-----------------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_df_to_json_bytes() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx)
        .await?
        .limit(0, Some(3))?
        .sort(vec![col("speed").sort(true, true)])?;
    let json_data = df_to_json_bytes(df).await?;
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;

    assert_eq!(
        serde_json::Value::Object(json_rows[0].clone()),
        serde_json::json!({"car": "red", "speed": 20.0, "time": "1996-04-12T12:05:03"}),
    );

    assert_eq!(
        serde_json::Value::Object(json_rows[1].clone()),
        serde_json::json!({"car": "red", "speed": 20.3, "time": "1996-04-12T12:05:04"}),
    );

    assert_eq!(
        serde_json::Value::Object(json_rows[2].clone()),
        serde_json::json!({"car": "red", "speed": 21.4, "time": "1996-04-12T12:05:05"}),
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
