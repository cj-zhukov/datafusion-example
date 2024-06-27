pub mod aws;
pub mod scalarvalue;
pub mod examples;
pub mod saved;

use std::io::{Cursor, Write};
use std::sync::Arc;

use anyhow::{Result, Context};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use datafusion::arrow::compute::concat;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use serde_json::{Map, Value};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_stream::StreamExt;
use futures_util::TryStreamExt;

// add auto-increment column to df
pub async fn add_pk_to_df(ctx: SessionContext, df: DataFrame, col_name: &str) -> Result<DataFrame> {
    let schema = df.schema().clone();
    let batches = df.collect().await?;
    let batches = batches.iter().collect::<Vec<_>>();
    let field_num = schema.fields().len();
    let mut arrays = Vec::with_capacity(field_num);
    for i in 0..field_num {
        let array = batches
            .iter()
            .map(|batch| batch.column(i).as_ref())
            .collect::<Vec<_>>();
        let array = concat(&array)?;
        arrays.push(array);
    }
    
    let max_len = arrays.first().unwrap().len();
    let pks: ArrayRef = Arc::new(Int32Array::from_iter(0..max_len as i32));
    arrays.push(pks);
    
    let schema_pks = Schema::new(vec![Field::new(col_name, DataType::Int32, true)]);
    let schema_new = Schema::try_merge(vec![schema.as_arrow().clone(), schema_pks])?;
    let batch = RecordBatch::try_new(schema_new.into(), arrays)?;
    let res = ctx.read_batch(batch)?;

    Ok(res)
}

// select all columns except cols_to_exclude
pub fn select_all_exclude(df: DataFrame, cols_to_exclude: &[&str]) -> Result<DataFrame> {
    let columns = df
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .filter(|x| !cols_to_exclude.iter().any(|col| col.eq(x)))
        .collect::<Vec<_>>();
    
    let res = df.clone().select_columns(&columns)?;

    Ok(res)
}

// get df columns names
pub fn get_column_names(df: DataFrame) -> Vec<String> {
    df
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().to_string())
        .collect::<Vec<_>>()
}

// concat arrays per column
pub async fn concat_arrays(df: DataFrame) -> Result<Vec<ArrayRef>> {
    let schema = df.schema().clone();
    let batches = df.collect().await?;
    let batches = batches.iter().collect::<Vec<_>>();
    let field_num = schema.fields().len();
    let mut arrays = Vec::with_capacity(field_num);
    for i in 0..field_num {
        let array = batches
            .iter()
            .map(|batch| batch.column(i).as_ref())
            .collect::<Vec<_>>();
        let array = concat(&array)?;
        
        arrays.push(array);
    }

    Ok(arrays)
}

// create json like string column
pub async fn df_cols_to_json(ctx: SessionContext, df: DataFrame, cols: &[&str], new_col: Option<&str>) -> Result<DataFrame> {
    let schema = df.schema().clone();
    let mut arrays = concat_arrays(df).await?;

    let batch = RecordBatch::try_new(schema.as_arrow().clone().into(), arrays.clone())?;
    let df_prepared = ctx.read_batch(batch)?;
    let batches = df_prepared.select_columns(cols)?.collect().await?;
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;

    let mut str_rows = vec![];
    for json_row in &json_rows {
        // let str_row = serde_json::to_string(&json_row)?;
        let str_row = if !json_rows.is_empty() {
            let str_row = serde_json::to_string(&json_row)?;
            Some(str_row)
        } else {
            None
        };
        str_rows.push(str_row);
    }

    let new_col_arr: ArrayRef = Arc::new(StringArray::from(str_rows));
    arrays.push(new_col_arr);

    let schema_new_col = Schema::new(vec![Field::new(new_col.unwrap_or("metadata"), DataType::Utf8, true)]);
    let schema_new = Schema::try_merge(vec![schema.as_arrow().clone(), schema_new_col])?;
    let batch = RecordBatch::try_new(schema_new.into(), arrays)?;
    let res = ctx.read_batch(batch)?;

    let res = select_all_exclude(res, cols)?;

    Ok(res)
}

// convert df columns to nested struct
pub async fn df_cols_to_struct(ctx: SessionContext, df: DataFrame, cols: &[&str], new_col: Option<&str>) -> Result<DataFrame> {
    let schema = df.schema().clone();
    let mut arrays = concat_arrays(df).await?;

    let batch = RecordBatch::try_new(schema.as_arrow().clone().into(), arrays.clone())?;
    let mut struct_array_data = vec![];
    for col in cols {
        let field = schema.as_arrow().field_with_name(col)?.clone();
        let arr = batch.column_by_name(col).unwrap().clone();
        struct_array_data.push((Arc::new(field), arr));
    }
    let df_struct = ctx.read_batch(batch.clone())?.select_columns(cols)?;
    let fields = df_struct.schema().clone().as_arrow().fields().clone();
    let struct_array = StructArray::from(struct_array_data);
    let struct_array_schema = Schema::new(vec![Field::new(new_col.unwrap_or("metadata"), DataType::Struct(fields), true)]);
    let schema_new = Schema::try_merge(vec![schema.as_arrow().clone(), struct_array_schema.clone()])?;
    arrays.push(Arc::new(struct_array));
    let batch_with_struct = RecordBatch::try_new(schema_new.into(), arrays)?;

    let res = ctx.read_batch(batch_with_struct)?;

    let res = select_all_exclude(res, cols)?;

    Ok(res)
}

pub async fn get_aws_client(region: &str) -> Result<Client> {
    let config = aws_config::defaults(BehaviorVersion::v2023_11_09())
        .region(Region::new(region.to_string()))
        .load()
        .await;

    let client = Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .retry_config(aws_config::retry::RetryConfig::standard()
            .with_max_attempts(10))
            .build()
    );

    Ok(client)
}

pub async fn read_file_to_df(ctx: SessionContext, file_path: &str) -> Result<DataFrame> {
    let mut buf = vec![];
    let _n = File::open(file_path).await?.read_to_end(&mut buf).await?;
    let stream = ParquetRecordBatchStreamBuilder::new(Cursor::new(buf))
        .await?
        .build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    let df = ctx.read_batches(batches)?;

    Ok(df)
}

pub async fn write_to_file(df: DataFrame, file_path: &str) -> Result<()> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await.context("could not create stream from df")?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None).context("could not create writer")?;
    while let Some(batch) = stream.next().await {
        let batch = batch.context("could not get record batch")?;
        writer.write(&batch).await.context("could not write to writer")?;
    }
    writer.close().await.context("could not close writer")?;

    let mut file = std::fs::File::create(file_path)?;
    file.write_all(&buf)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{arrow::array::Array, assert_batches_eq};

    #[test]
    fn test_get_column_names() {
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
        let df = ctx.read_batch(batch.clone()).unwrap();
        let cols = get_column_names(df);
        assert_eq!(cols, vec!["id", "name", "data"]);
    }

    #[test]
    fn test_convert_cols_to_json() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
    
        let buf = vec![];
        let mut writer = arrow_json::ArrayWriter::new(buf);
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    
        let json_data = writer.into_inner();
        let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice()).unwrap();
    
        assert_eq!(
            serde_json::Value::Object(json_rows[1].clone()),
            serde_json::json!({"a": 2}),
        );
    }

    #[tokio::test]
    async fn test_concat_arrays() {
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
        let df = ctx.read_batch(batch.clone()).unwrap();
        let arrays = concat_arrays(df).await.unwrap();
        assert_eq!(arrays.len(), 3);

        let ids = arrays.get(0).unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);

        let names = arrays.get(1).unwrap().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "foo");
        assert_eq!(names.value(1), "bar");
        assert_eq!(names.value(2), "baz");

        let data_all = arrays.get(2).unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(data_all.values(), &[42, 43, 44]);
    }

    #[tokio::test]
    async fn test_cols_to_json() {
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
        let df = ctx.read_batch(batch.clone()).unwrap();
        let res = df_cols_to_json(ctx, df, &["name", "data"], Some("metadata")).await.unwrap();

        assert_eq!(res.schema().fields().len(), 2); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 3); // rows count

        let rows = res.sort(vec![col("id").sort(true, true)]).unwrap();
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
            &rows.collect().await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_cols_to_struct() {
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
        let df = ctx.read_batch(batch.clone()).unwrap();
        let res = df_cols_to_struct(ctx, df, &["name", "data"], Some("metadata")).await.unwrap();

        assert_eq!(res.schema().fields().len(), 2); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 3); // rows count

        let rows = res.sort(vec![col("id").sort(true, true)]).unwrap();
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
            &rows.collect().await.unwrap()
        );
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
            schema.clone().into(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                Arc::new(Int32Array::from(vec![42, 43, 44])),
            ],
        ).unwrap();
    
        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch.clone()).unwrap();
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

    #[tokio::test]
    async fn test_add_pk_to_df() {
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
        let df = ctx.read_batch(batch.clone()).unwrap();

        let res = add_pk_to_df(ctx, df, "pk").await.unwrap();

        assert_eq!(res.schema().fields().len(), 4); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 3); // rows count
        
        let rows = res.sort(vec![col("id").sort(true, true)]).unwrap();
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
            &rows.collect().await.unwrap()
        );
    }
}