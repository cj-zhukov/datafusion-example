use std::io::Error as IoError;

use aws_sdk_s3::error::SdkError;
use aws_smithy_types::byte_stream::error::Error as AWSSmithyError;
use awscreds::error::CredentialsError as AWSCredentialsError;
use color_eyre::eyre::Report;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use object_store::Error as ObjectStoreError;
use parquet::errors::ParquetError;
use serde_json::error::Error as SerdeError;
use thiserror::Error;
use url::ParseError as UrlParseError;

#[derive(Debug, Error)]
pub enum UtilsError {
    #[error("ArrowError")]
    ArrowError(#[from] ArrowError),

    #[error("AWSSmithyError")]
    AWSSmithyError(#[from] AWSSmithyError),

    #[error("IO error")]
    IoError(#[from] IoError),

    #[error("AWS S3 error")]
    AwsS3(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("ObjectStoreError")]
    ObjectStoreError(#[from] ObjectStoreError),

    #[error("AWSCredentialsError")]
    AWSCredentialsError(#[from] AWSCredentialsError),

    #[error("DataFusionError")]
    DataFusionError(#[from] DataFusionError),

    #[error("ParquetError")]
    ParquetError(#[from] ParquetError),

    #[error("SerdeError")]
    SerdeError(#[from] SerdeError),

    #[error("UrlParseError")]
    UrlParseError(#[from] UrlParseError),

    #[error("Unexpected error")]
    UnexpectedError(#[source] Report),
}

impl<E> From<SdkError<E>> for UtilsError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: SdkError<E>) -> Self {
        UtilsError::AwsS3(Box::new(err))
    }
}
