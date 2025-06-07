use color_eyre::Result;
use datafusion::assert_batches_eq;
use datafusion::prelude::*;

use datafusion_example::utils::conversions::df_from_columns;

#[tokio::test]
async fn test_df_from_columns_arr() -> Result<()> {
    let id = Box::new([1, 2, 3]);
    let data = Box::new([42, 43, 44]);
    let name = Box::new(["foo", "bar", "baz"]);
    let df = df_from_columns(vec![("id", id), ("data", data), ("name", name)])?;

    assert_eq!(df.schema().fields().len(), 3); // columns count
    assert_eq!(df.clone().count().await?, 3); // rows count

    let rows = df.sort(vec![col("id").sort(true, true)])?;
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
async fn test_df_from_columns_vec() -> Result<()> {
    let id = Box::new(vec![1, 2, 3]);
    let data = Box::new(vec![42, 43, 44]);
    let name = Box::new(vec!["foo", "bar", "baz"]);
    let df = df_from_columns(vec![("id", id), ("data", data), ("name", name)])?;

    assert_eq!(df.schema().fields().len(), 3); // columns count
    assert_eq!(df.clone().count().await?, 3); // rows count

    let rows = df.sort(vec![col("id").sort(true, true)])?;
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
