pub mod scalarvalue;
pub mod examples;
pub mod saved;

use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::Arc;

use anyhow::{Context, Result};
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use awscreds::Credentials;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use datafusion::arrow::compute::concat;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use serde_json::{Map, Value};
use itertools::Itertools;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_stream::StreamExt;
use futures_util::TryStreamExt;
use url::Url;

// add auto-increment column to df
pub async fn add_pk_to_df(ctx: SessionContext, df: DataFrame, col_name: &str) -> Result<DataFrame> {
    let schema = df.schema().clone();
    let batches = df.collect().await?;
    let batches = batches.iter().map(|x| x).collect::<Vec<_>>();
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
    
    let max_len = arrays.get(0).unwrap().len();
    let pks: ArrayRef = Arc::new(Int32Array::from_iter(0..max_len as i32));
    arrays.push(pks);
    
    let schema_pks = Schema::new(vec![Field::new(col_name, DataType::Int32, true)]);
    let schema_new = Schema::try_merge(vec![schema.as_arrow().clone(), schema_pks])?;
    let batch = RecordBatch::try_new(schema_new.into(), arrays)?;
    let res = ctx.read_batch(batch)?;

    Ok(res)
}

// select all columns except cols_to_exclude
pub async fn select_all_exclude(df: DataFrame, cols_to_exclude: &[&str]) -> Result<DataFrame> {
    let columns = df
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .filter(|x| cols_to_exclude.iter().find(|col| col.eq(&x)).is_none())
        .collect::<Vec<_>>();
    
    let res = df.clone().select_columns(&columns)?;

    Ok(res)
}

pub fn get_column_names(df: DataFrame) -> Vec<String> {
    df
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().to_string())
        .collect::<Vec<_>>()
}

// create json like string column
pub async fn df_cols_to_json(ctx: SessionContext, df: DataFrame, cols: &[&str], new_col: Option<&str>) -> Result<DataFrame> {
    let schema = df.schema().clone();
    let batches = df.collect().await?;
    let batches = batches.iter().map(|x| x).collect::<Vec<_>>();
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
        let str_row = if json_row.len() > 0 {
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

    let res = select_all_exclude(res, cols).await?;

    Ok(res)
}

// create json like string column using joining
pub async fn df_cols_to_json2(ctx: SessionContext, df: DataFrame, cols: &[&str], new_col: Option<&str>) -> Result<DataFrame> {
    let pk = "pk";
    let df = add_pk_to_df(ctx.clone(), df, pk).await?;
    let mut cols_new = cols.iter().map(|x| x.to_owned()).collect::<Vec<_>>();
    cols_new.push(pk);
    
    let df_cols_for_json = df.clone().select_columns(&cols_new)?;
    let mut stream = df_cols_for_json.clone().execute_stream().await.context("could not create stream")?;
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch)?;
    }
    writer.finish()?;
    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;
    let mut res = HashMap::new();
    for mut json in json_rows {
        let pk = json.remove(pk).unwrap().to_string().parse::<i32>()?;
        res.extend(HashMap::from([(pk, json)]));
    }
    // println!("res:{:?}", res)
    let mut primary_keys = vec![];
    let mut data_all = vec![];
    for i in res.keys().sorted() {
        primary_keys.push(*i);
        let row = res[i].clone();
        // add Option type for json string like col
        let str_row = if row.len() > 0 {
            let str_row = serde_json::to_string(&row)?;
            Some(str_row)
        } else {
            None
        };
        data_all.push(str_row);
    }

    let mut right_cols = pk.to_string();
    right_cols.push_str("tojoin");
    let schema = Schema::new(vec![
        Field::new(right_cols.clone(), DataType::Int32, false),
        Field::new(new_col.unwrap_or("metadata"), DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(primary_keys)),
            Arc::new(StringArray::from(data_all)),
        ],
    )?;
    let df_to_json = ctx.read_batch(batch.clone())?;
    
    let res = df.join(df_to_json, JoinType::Inner, &[pk], &[&right_cols], None)?;
    
    let columns = res
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .filter(|x| !x.contains("tojoin"))
        .filter(|x| cols_new.iter().find(|col| col.eq(&x)).is_none())
        .collect::<Vec<_>>();

    let res = res.clone().select_columns(&columns)?;

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

pub async fn read_from_s3(ctx: SessionContext, bucket: &str, region: &str, key: &str) -> Result<()> {
    let creds = Credentials::default()?;
    let aws_access_key_id = creds.access_key.unwrap();
    let aws_secret_access_key = creds.secret_key.unwrap();
    let aws_session_token = creds.session_token.unwrap();

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(aws_access_key_id)
        .with_secret_access_key(aws_secret_access_key)
        .with_token(aws_session_token)
        .build()?;

    let path = format!("s3://{bucket}");
    let s3_url = Url::parse(&path)?;
    ctx.runtime_env().register_object_store(&s3_url, Arc::new(s3));

    let path = format!("s3://{bucket}/{key}");
    ctx.register_parquet("foo", &path, ParquetReadOptions::default()).await?;
    let df = ctx.sql("select * from foo").await?;
    df.show().await?;

    Ok(())
}

pub async fn write_to_s3(ctx: SessionContext, bucket: &str, region: &str, key: &str, df: DataFrame) -> Result<()> {
    let creds = Credentials::default()?;
    let aws_access_key_id = creds.access_key.unwrap();
    let aws_secret_access_key = creds.secret_key.unwrap();
    let aws_session_token = creds.session_token.unwrap();

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(aws_access_key_id)
        .with_secret_access_key(aws_secret_access_key)
        .with_token(aws_session_token)
        .build()?;

    let path = format!("s3://{bucket}");
    let s3_url = Url::parse(&path)?;
    ctx.runtime_env().register_object_store(&s3_url, Arc::new(s3));

    // read from s3 file to df
    // let path = format!("s3://{bucket}/path/to/data/");
    // let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
    // let listing_options = ListingOptions::new(Arc::new(file_format)).with_file_extension(FileType::PARQUET.get_ext());
    // ctx.register_listing_table("foo", &path, listing_options, None, None).await?;
    // let df = ctx.sql("select * from foo").await?;

    let batches = df.collect().await?;
    let df = ctx.read_batches(batches)?;
    let out_path = format!("s3://{bucket}/{key}");
    df.write_parquet(&out_path, DataFrameWriteOptions::new(), None).await.context("could not write to s3")?;

    Ok(())
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

pub async fn write_df_to_s3(client: Client, bucket: &str, key: &str, df: DataFrame) -> Result<()> {
    let mut buf = vec![];
    // let props = default_builder(&ConfigOptions::default())?.build();
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await.context("could not create stream from df")?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None).context("could not create writer")?;
    while let Some(batch) = stream.next().await {
        let batch = batch.context("could not get record batch")?;
        writer.write(&batch).await.context("could not write to writer")?;
    }
    writer.close().await.context("could not close writer")?;

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context(format!("could not create multipart upload bucket: {} key: {}", bucket, key))?;

    let upload_id = multipart_upload_res.upload_id().context(format!("could not get upload_id for key: {}", key))?;
    let mut upload_parts: Vec<CompletedPart> = Vec::new();
    let mut stream = ByteStream::from(buf);
    let mut part_number = 1;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        let upload_part_res = client
            .upload_part()
            .key(key)
            .bucket(bucket)
            .upload_id(upload_id)
            .body(ByteStream::from(bytes))
            .part_number(part_number)
            .send()
            .await
            .context(format!("could not create upload part for key: {}", key))?;
    
        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );

        part_number += 1;
    }

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await
        .context(format!("could not complete multipart upload for key: {}", key))?;

    Ok(())
}

pub async fn write_batches_to_s3(client: Client, bucket: &str, key: &str, batches: Vec<RecordBatch>) -> Result<()> {
    let mut buf = vec![];
    let schema = batches[0].schema();
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema, None).context("could not create writer")?;
    for batch in batches {
        writer.write(&batch).await.context("could not write to writer")?;
    }
    writer.close().await.context("could not close writer")?;

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context(format!("could not create multipart upload bucket: {} key: {}", bucket, key))?;

    let upload_id = multipart_upload_res.upload_id().context(format!("could not get upload_id for key: {}", key))?;
    let mut upload_parts: Vec<CompletedPart> = Vec::new();
    let mut stream = ByteStream::from(buf);
    let mut part_number = 1;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        let upload_part_res = client
            .upload_part()
            .key(key)
            .bucket(bucket)
            .upload_id(upload_id)
            .body(ByteStream::from(bytes))
            .part_number(part_number)
            .send()
            .await
            .context(format!("could not create upload part for key: {}", key))?;
    
        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );

        part_number += 1;
    }

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await
        .context(format!("could not complete multipart upload for key: {}", key))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;

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

        let row1 = res.clone().filter(col("id").eq(lit(1))).unwrap();
        assert_batches_eq!(
            &[
                  "+----+--------------------------+",
                  "| id | metadata                 |",
                  "+----+--------------------------+",
                r#"| 1  | {"data":42,"name":"foo"} |"#,
                  "+----+--------------------------+",
            ],
            &row1.collect().await.unwrap()
        );

        let row2 = res.clone().filter(col("id").eq(lit(2))).unwrap();
        assert_batches_eq!(
            &[
                  "+----+--------------------------+",
                  "| id | metadata                 |",
                  "+----+--------------------------+",
                r#"| 2  | {"data":43,"name":"bar"} |"#,
                  "+----+--------------------------+",
            ],
            &row2.collect().await.unwrap()
        );

        let row3 = res.clone().filter(col("id").eq(lit(3))).unwrap();
        assert_batches_eq!(
            &[
                  "+----+--------------------------+",
                  "| id | metadata                 |",
                  "+----+--------------------------+",
                r#"| 3  | {"data":44,"name":"baz"} |"#,
                  "+----+--------------------------+",
            ],
            &row3.collect().await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_cols_to_json2() {
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
        let res = df_cols_to_json2(ctx, df, &["name", "data"], Some("metadata")).await.unwrap();

        assert_eq!(res.schema().fields().len(), 2); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 3); // rows count

        let row1 = res.clone().filter(col("id").eq(lit(1))).unwrap();
        assert_batches_eq!(
            &[
                  "+----+--------------------------+",
                  "| id | metadata                 |",
                  "+----+--------------------------+",
                r#"| 1  | {"data":42,"name":"foo"} |"#,
                  "+----+--------------------------+",
            ],
            &row1.collect().await.unwrap()
        );

        let row2 = res.clone().filter(col("id").eq(lit(2))).unwrap();
        assert_batches_eq!(
            &[
                  "+----+--------------------------+",
                  "| id | metadata                 |",
                  "+----+--------------------------+",
                r#"| 2  | {"data":43,"name":"bar"} |"#,
                  "+----+--------------------------+",
            ],
            &row2.collect().await.unwrap()
        );

        let row3 = res.clone().filter(col("id").eq(lit(3))).unwrap();
        assert_batches_eq!(
            &[
                  "+----+--------------------------+",
                  "| id | metadata                 |",
                  "+----+--------------------------+",
                r#"| 3  | {"data":44,"name":"baz"} |"#,
                  "+----+--------------------------+",
            ],
            &row3.collect().await.unwrap()
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
        let res = select_all_exclude(df, &["pkey", "data"]).await.unwrap();
        
        assert_eq!(res.schema().fields().len(), 2); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 3); // rows count

        let row1 = res.clone().filter(col("id").eq(lit(1))).unwrap();
        assert_batches_eq!(
            &[
                  "+----+------+",
                  "| id | name |",
                  "+----+------+",
                  "| 1  | foo  |",
                  "+----+------+",
            ],
            &row1.collect().await.unwrap()
        );

        let row2 = res.clone().filter(col("id").eq(lit(2))).unwrap();
        assert_batches_eq!(
            &[
                "+----+------+",
                "| id | name |",
                "+----+------+",
                "| 2  | bar  |",
                "+----+------+",
          ],
            &row2.collect().await.unwrap()
        );

        let row3 = res.clone().filter(col("id").eq(lit(3))).unwrap();
        assert_batches_eq!(
            &[
                "+----+------+",
                "| id | name |",
                "+----+------+",
                "| 3  | baz  |",
                "+----+------+",
          ],
            &row3.collect().await.unwrap()
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
        
        let row1 = res.clone().filter(col("id").eq(lit(1))).unwrap();
        assert_batches_eq!(
            &[
                "+----+------+------+----+",
                "| id | name | data | pk |",
                "+----+------+------+----+",
                "| 1  | foo  | 42   | 0  |",
                "+----+------+------+----+",
            ],
            &row1.collect().await.unwrap()
        );

        let row2 = res.clone().filter(col("id").eq(lit(2))).unwrap();
        assert_batches_eq!(
            &[
                "+----+------+------+----+",
                "| id | name | data | pk |",
                "+----+------+------+----+",
                "| 2  | bar  | 43   | 1  |",
                "+----+------+------+----+",
            ],
            &row2.collect().await.unwrap()
        );

        let row3 = res.clone().filter(col("id").eq(lit(3))).unwrap();
        assert_batches_eq!(
            &[
                "+----+------+------+----+",
                "| id | name | data | pk |",
                "+----+------+------+----+",
                "| 3  | baz  | 44   | 2  |",
                "+----+------+------+----+",
            ],
            &row3.collect().await.unwrap()
        );
    }
}