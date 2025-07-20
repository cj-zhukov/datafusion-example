use std::io::Cursor;
use std::sync::Arc;

use aws_config::{BehaviorVersion, Region, retry::RetryConfig};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Builder;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use awscreds::Credentials;
use bytes::Bytes;
use color_eyre::eyre::Report;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use futures_util::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use url::Url;

use crate::error::UtilsError;
use crate::utils::constants::*;

pub async fn get_aws_client(region: String) -> Client {
    let region = Region::new(region);

    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .load()
        .await;

    let config_builder = Builder::from(&sdk_config)
        .retry_config(RetryConfig::standard().with_max_attempts(AWS_MAX_RETRIES));

    let config = config_builder.build();

    Client::from_conf(config)
}

/// Get aws GetObjectOutput
pub async fn get_aws_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<GetObjectOutput, UtilsError> {
    let req = client.get_object().bucket(bucket).key(key);
    let res = req.send().await?;
    Ok(res)
}

/// Read file from aws s3
pub async fn read_file(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>, UtilsError> {
    let mut buf = Vec::new();
    let mut object = get_aws_object(client, bucket, key).await?;
    while let Some(bytes) = object.body.try_next().await? {
        buf.extend(bytes.to_vec());
    }
    Ok(buf)
}

/// Get file from AWS S3 based on file size
pub async fn read_file2(
    client: &Client,
    bucket: &str,
    key: &str,
    chunk_suze: Option<u64>,
) -> Result<Vec<u8>, UtilsError> {
    let object = get_aws_object(client, bucket, key).await?;
    let length = object.content_length().unwrap_or(0) as u64;
    let mut body = object.body;
    if length <= chunk_suze.unwrap_or(CHUNK_SIZE) {
        let mut buf = Vec::with_capacity(length as usize);
        let mut reader = body.into_async_read();
        reader.read_to_end(&mut buf).await?;
        Ok(buf)
    } else {
        let mut buf = Vec::with_capacity(length as usize);
        while let Some(chunk) = body.try_next().await? {
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }
}

/// Get file from AWS S3 by reading chunks in parallel
pub async fn read_file_big(
    client: &Client,
    bucket: &str,
    key: &str,
    chunk_size: Option<u64>,
    chunks_workers: Option<usize>,
) -> Result<Vec<u8>, UtilsError> {
    let object = get_aws_object(client, bucket, key).await?;
    let size = object.content_length().unwrap_or(0) as u64;
    let mut ranges = vec![];
    for start in (0..size).step_by(chunk_size.unwrap_or(CHUNK_SIZE) as usize) {
        let end = (start + chunk_size.unwrap_or(CHUNK_SIZE) - 1).min(size - 1);
        ranges.push((start, end));
    }

    let semaphore = Arc::new(Semaphore::new(chunks_workers.unwrap_or(CHUNKS_WORKERS)));
    let mut tasks = vec![];
    let ranges_len = ranges.clone().len();
    for (i, (start, end)) in ranges.into_iter().enumerate() {
        let client = client.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| UtilsError::UnexpectedError(e.into()))?;

        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let range = format!("bytes={}-{}", start, end);
            let out = client
                .get_object()
                .bucket(&bucket)
                .key(&key)
                .range(range)
                .send()
                .await?;
            let bytes = out.body.collect().await?.into_bytes();
            Ok::<(usize, Bytes), UtilsError>((i, bytes))
        }));
    }

    let mut results = vec![Bytes::new(); ranges_len];
    for task in tasks {
        let (i, chunk) = task
            .await
            .map_err(|e| UtilsError::UnexpectedError(e.into()))?
            .map_err(|e| UtilsError::UnexpectedError(e.into()))?;
        results[i] = chunk;
    }

    let total_size: usize = results.iter().map(|b| b.len()).sum();
    let mut buf = Vec::with_capacity(total_size);
    for chunk in results {
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// Read parquet file to dataframe
pub async fn read_file_to_df(
    client: &Client,
    ctx: &SessionContext,
    bucket: &str,
    key: &str,
) -> Result<DataFrame, UtilsError> {
    let buf = read_file(client, bucket, key).await?;
    let stream = ParquetRecordBatchStreamBuilder::new(Cursor::new(buf))
        .await?
        .build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    let res = ctx.read_batches(batches)?;
    Ok(res)
}

/// Read parquet file or dir from AWS S3 into dataframe
pub async fn read_from_s3(
    ctx: &SessionContext,
    region: &str,
    bucket: &str,
    key: &str,
) -> Result<DataFrame, UtilsError> {
    let creds = Credentials::default()?;
    let aws_access_key_id = creds.access_key.unwrap_or_default();
    let aws_secret_access_key = creds.secret_key.unwrap_or_default();
    let aws_session_token = creds.session_token.unwrap_or_default();

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(aws_access_key_id)
        .with_secret_access_key(aws_secret_access_key)
        .with_token(aws_session_token)
        .build()?;

    let path = format!("s3://{bucket}");
    let s3_url = Url::parse(&path)?;
    ctx.runtime_env()
        .register_object_store(&s3_url, Arc::new(s3));

    let path = format!("s3://{bucket}/{key}");
    ctx.register_parquet("t", &path, ParquetReadOptions::default())
        .await?;
    let res = ctx.sql("select * from t").await?;
    Ok(res)
}

pub async fn write_to_s3(
    ctx: &SessionContext,
    bucket: &str,
    region: &str,
    key: &str,
    df: DataFrame,
) -> Result<(), UtilsError> {
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
    ctx.runtime_env()
        .register_object_store(&s3_url, Arc::new(s3));

    // read from s3 file to df
    // let path = format!("s3://{bucket}/path/to/data/");
    // let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
    // let listing_options = ListingOptions::new(Arc::new(file_format)).with_file_extension(FileType::PARQUET.get_ext());
    // ctx.register_listing_table("foo", &path, listing_options, None, None).await?;
    // let df = ctx.sql("select * from foo").await?;

    let batches = df.collect().await?;
    let df = ctx.read_batches(batches)?;
    let out_path = format!("s3://{bucket}/{key}");
    df.write_parquet(&out_path, DataFrameWriteOptions::new(), None)
        .await?;
    Ok(())
}

/// Write dataframe to aws s3 by chunk
pub async fn write_df_to_s3(
    client: &Client,
    bucket: &str,
    key: &str,
    df: DataFrame,
) -> Result<(), UtilsError> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None)?;
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch).await?;
    }
    writer.close().await?;

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = multipart_upload_res
        .upload_id()
        .ok_or_else(|| UtilsError::UnexpectedError(Report::msg("missing upload_id")))?;

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
            .await?;

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

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await?;

    Ok(())
}

/// Write big dataframe to aws s3 by chunk
pub async fn write_big_df_to_s3(
    client: &Client,
    bucket: &str,
    key: &str,
    df: DataFrame,
    max_workers: usize,
) -> Result<(), UtilsError> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None)?;
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch).await?;
    }
    writer.close().await?;

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = multipart_upload_res
        .upload_id()
        .ok_or_else(|| UtilsError::UnexpectedError(Report::msg("missing upload_id")))?;

    let parts: Vec<(usize, Bytes)> = buf
        .chunks(CHUNK_SIZE as usize)
        .enumerate()
        .map(|(i, chunk)| (i + 1, Bytes::copy_from_slice(chunk)))
        .collect();

    let semaphore = Arc::new(Semaphore::new(max_workers));
    let mut tasks = JoinSet::new();

    for (part_number, chunk) in parts {
        let client = client.clone();
        let upload_id = upload_id.to_string();
        let key = key.to_string();
        let bucket = bucket.to_string();
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| UtilsError::UnexpectedError(e.into()))?;

        tasks.spawn(async move {
            let res = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id.clone())
                .part_number(part_number as i32)
                .body(ByteStream::from(chunk.clone()))
                .send()
                .await?;

            drop(permit);

            Ok::<CompletedPart, aws_sdk_s3::Error>(
                CompletedPart::builder()
                    .e_tag(res.e_tag().unwrap_or_default())
                    .part_number(part_number as i32)
                    .build(),
            )
        });
    }

    let mut completed_parts = Vec::new();
    while let Some(res) = tasks.join_next().await {
        let part = res
            .map_err(|e| UtilsError::UnexpectedError(e.into()))?
            .map_err(|e| UtilsError::UnexpectedError(e.into()))?;
        completed_parts.push(part);
    }

    completed_parts.sort_by_key(|part| part.part_number());
    let completed_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed_upload)
        .send()
        .await?;

    Ok(())
}

/// Write dataframe's record batches
pub async fn write_batches_to_s3(
    client: &Client,
    bucket: &str,
    key: &str,
    batches: Vec<RecordBatch>,
) -> Result<(), UtilsError> {
    let mut buf = vec![];
    let schema = batches[0].schema();
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema, None)?;
    for batch in batches {
        writer.write(&batch).await?;
    }
    writer.close().await?;

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = multipart_upload_res
        .upload_id()
        .ok_or_else(|| UtilsError::UnexpectedError(Report::msg("missing upload_id")))?;

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
            .await?;

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
        .await?;

    Ok(())
}
