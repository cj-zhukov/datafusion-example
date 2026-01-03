use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::prelude::*;
use datafusion_example::utils::dataframe::register_df_view;

pub async fn udf_example() -> Result<()> {
    split_name().await?;
    split_name_sql().await?;
    Ok(())
}

// split foo.txt by . and get name
async fn split_name() -> Result<()> {
    let split_text = Arc::new(|args: &[ColumnarValue]| {
        let input = match &args[0] {
            ColumnarValue::Array(array) => array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution("Invalid input array".to_string())
                })?,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Expected array as input".to_string(),
                ));
            }
        };

        let result: StringArray = input
            .iter()
            .map(|maybe_str| {
                maybe_str.and_then(|s| s.split('.').next().map(|part| part.to_string()))
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    });

    let udf = create_udf(
        "split_text",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        split_text,
    );

    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo.txt", "bar.txt", "baz.txt"]
    )?;
    let expr = udf.call(vec![col("name")]);
    let res = df.select(vec![col("id"), expr.alias("name_without_extension")])?;
    res.show().await?;
    Ok(())
}

// split foo.txt by . and get extension
async fn split_name_sql() -> Result<()> {
    let split_text = Arc::new(|args: &[ColumnarValue]| {
        let input = match &args[0] {
            ColumnarValue::Array(array) => array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution("Invalid input array".to_string())
                })?,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Expected array as input".to_string(),
                ));
            }
        };

        let result: StringArray = input
            .iter()
            .map(|maybe_str| {
                maybe_str.and_then(|s| s.split('.').nth(1).map(|part| part.to_string()))
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    });

    let udf = create_udf(
        "split_text",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        split_text,
    );

    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo.txt", "bar.txt", "baz.txt"]
    )?;
    register_df_view(&ctx, &df, "t")?;
    ctx.register_udf(udf);
    let res = ctx
        .sql("select id, split_text(name) as extension from t")
        .await?;
    res.show().await?;
    Ok(())
}
