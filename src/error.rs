use std::io::Error as IoError;

use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_smithy_types::byte_stream::error::Error as AWSSmithyError;
use awscreds::error::CredentialsError as AWSCredentialsError;
use color_eyre::eyre::Report;
use datafusion::error::DataFusionError;
use datafusion::arrow::error::ArrowError;
use object_store::Error as ObjectStoreError;
use parquet::errors::ParquetError;
use serde_json::error::Error as SerdeError;
use url::ParseError as UrlParseError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UtilsError {
    #[error("ArrowError")]
    ArrowError(#[from] ArrowError),

    #[error("AWSSmithyError")]
    AWSSmithyError(#[from] AWSSmithyError),

    #[error("IO error")]
    IoError(#[from] IoError),

    #[error("AWS GetObjectError")]
    GetObjectError(#[from] SdkError<GetObjectError>),

    #[error("AWS ListObjectsV2Error")]
    ListObjectsV2Error(#[from] SdkError<ListObjectsV2Error>),

    #[error("ObjectStoreError")]
    ObjectStoreError(#[from] ObjectStoreError),

    #[error("AWS CreateMultipartUploadError")]
    CreateMultipartUploadError(#[from] SdkError<CreateMultipartUploadError>),

    #[error("AWSCredentialsError")]
    AWSCredentialsError(#[from] AWSCredentialsError),

    #[error("AWS CompleteMultipartUploadError")]
    CompleteMultipartUploadError(#[from] SdkError<CompleteMultipartUploadError>),

    #[error("DataFusionError")]
    DataFusionError(#[from] DataFusionError),

    #[error("ParquetError")]
    ParquetError(#[from] ParquetError),

    #[error("AWS PutObjectError")]
    PutObjectError(#[from] SdkError<PutObjectError>),

    #[error("AWS UploadPartError")]
    UploadPartError(#[from] SdkError<UploadPartError>),

    #[error("SerdeError")]
    SerdeError(#[from] SerdeError),

    #[error("UrlParseError")]
    UrlParseError(#[from] UrlParseError),
    
    #[error("Unexpected error")]
    UnexpectedError(#[source] Report)
}
