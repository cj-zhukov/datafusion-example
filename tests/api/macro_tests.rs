use color_eyre::Result;
use datafusion::assert_batches_eq;
use datafusion::prelude::*;

use datafusion_example::df;

#[tokio::test]
async fn test_df_macro() -> Result<()> {
    let df = df!(
        "id" => vec![1, 2, 3],
        "data" => vec![42, 43, 44],
        "name" => vec![Some("foo"), Some("bar"), None]
    );

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
            "| 3  | 44   |      |",
            "+----+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}
