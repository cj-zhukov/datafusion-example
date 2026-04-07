use std::{path::{Path, PathBuf}, sync::Arc};

use color_eyre::eyre::Report;
use datafusion::{arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit}, dataframe::DataFrameWriteOptions};
use datafusion::prelude::*;
use tempfile::TempDir;
use tokio::fs::create_dir_all;

use crate::error::UtilsError;

#[derive(Debug)]
pub enum ExampleDataset {
    Cars,
}

impl ExampleDataset {
    pub fn file_stem(&self) -> &'static str {
        match self {
            Self::Cars => "cars",
        }
    }

    pub fn path(&self) -> PathBuf {
        PathBuf::new()
            .join("data")
            .join("csv")
            .join(format!("{}.csv", self.file_stem()))
    }

    pub fn path_str(&self) -> Result<String, UtilsError> {
        let path = self.path();
        path.to_str().map(String::from).ok_or_else(|| {
            UtilsError::UnexpectedError(Report::msg(format!(
                "CSV directory path is not valid UTF-8: {}",
                path.display()
            )))
        })
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Cars => Arc::new(Schema::new(vec![
                Field::new("car", DataType::Utf8, false),
                Field::new("speed", DataType::Float64, false),
                Field::new(
                    "time",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
            ])),
        }
    }

    pub async fn dataframe(self, ctx: &SessionContext) -> Result<DataFrame, UtilsError> {
        Ok(ctx.read_csv(self.path_str()?, CsvReadOptions::default()).await?)
    }
}

/// Temporary Parquet directory that is deleted when dropped.
#[derive(Debug)]
pub struct ParquetTemp {
    pub tmp_dir: TempDir,
    pub parquet_dir: PathBuf,
}

impl ParquetTemp {
    pub fn path(&self) -> &Path {
        &self.parquet_dir
    }

    pub fn path_str(&self) -> Result<&str, UtilsError> {
        self.parquet_dir.to_str().ok_or_else(|| {
            UtilsError::UnexpectedError(Report::msg(format!(
                "Parquet directory path is not valid UTF-8: {}",
                self.parquet_dir.display()
            )))
        })
    }

    pub fn file_uri(&self) -> Result<String, UtilsError> {
        Ok(format!("file://{}", self.path_str()?))
    }
}

/// Helper for examples: load a CSV file and materialize it as Parquet
/// in a temporary directory.
pub async fn write_csv_to_parquet(
    ctx: &SessionContext,
    csv_path: &Path,
) -> Result<ParquetTemp, UtilsError> {
    if !csv_path.is_file() {
        return Err(UtilsError::UnexpectedError(Report::msg(format!(
            "CSV file does not exist: {}",
            csv_path.display()
        ))));
    }

    let csv_path = csv_path.to_str().ok_or_else(|| {
        UtilsError::UnexpectedError(Report::msg("CSV path is not valid UTF-8".to_string()))
    })?;

    let csv_df = ctx.read_csv(csv_path, CsvReadOptions::default()).await?;

    let tmp_dir = TempDir::new()?;
    let parquet_dir = tmp_dir.path().join("parquet_source");
    create_dir_all(&parquet_dir).await?;

    let path = parquet_dir.to_str().ok_or_else(|| {
        UtilsError::UnexpectedError(Report::msg("Failed processing tmp directory path".to_string()))
    })?;

    csv_df
        .write_parquet(path, DataFrameWriteOptions::default(), None)
        .await?;

    Ok(ParquetTemp {
        tmp_dir,
        parquet_dir,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use datafusion::assert_batches_eq;
    
    #[tokio::test]
    async fn test_cars_dataset_dataframe() -> Result<(), UtilsError> {
        let cars = ExampleDataset::Cars;
        let ctx = SessionContext::new();
        let df = cars.dataframe(&ctx).await?;
        let rows = df.sort(vec![col("speed").sort(true, true)])?;
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
    async fn test_write_csv_to_parquet_with_cars_data() -> Result<(), UtilsError> {
        let ctx = SessionContext::new();
        let csv_path = PathBuf::new()
            .join("data")
            .join("csv")
            .join("cars.csv");

        let parquet_dir = write_csv_to_parquet(&ctx, &csv_path).await?;
        let df = ctx
            .read_parquet(parquet_dir.path_str()?, ParquetReadOptions::default())
            .await?;

        let rows = df.sort(vec![col("speed").sort(true, true)])?;
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
    async fn test_write_csv_to_parquet_error() {
        let ctx = SessionContext::new();
        let csv_path = PathBuf::new()
            .join("data")
            .join("csv")
            .join("file-does-not-exist.csv");

        let err = write_csv_to_parquet(&ctx, &csv_path).await.unwrap_err();
        match err {
            UtilsError::UnexpectedError(msg) => {
                assert!(
                    msg.to_string().contains("CSV file does not exist"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
