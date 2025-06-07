use color_eyre::Result;
use datafusion::assert_batches_eq;
use datafusion::prelude::*;

use datafusion_example::df;
use datafusion_example::df_try;

#[tokio::test]
async fn test_df_macro_vec() -> Result<()> {
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

#[tokio::test]
async fn test_df_macro_arr() -> Result<()> {
    let df = df!(
        "id" => [1, 2, 3],
        "data" => [42, 43, 44],
        "name" => [Some("foo"), Some("bar"), None]
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

#[tokio::test]
async fn test_df_try_macro_vec() -> Result<()> {
    let df = df_try!(
        "id" => vec![1, 2, 3],
        "data" => vec![42, 43, 44],
        "name" => vec![Some("foo"), Some("bar"), None]
    )?;

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

#[tokio::test]
async fn test_df_try_macro_arr() -> Result<()> {
    let df = df_try!(
        "id" => [1, 2, 3],
        "data" => [42, 43, 44],
        "name" => [Some("foo"), Some("bar"), None]
    )?;

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

#[tokio::test]
async fn test_df_macro_empty() -> Result<()> {
    let df = df!();
    assert_eq!(df.schema().fields().len(), 0); // columns count
    assert_eq!(df.clone().count().await?, 0); // rows count
    Ok(())
}

#[tokio::test]
async fn test_df_try_macro_empty() -> Result<()> {
    let df = df_try!()?;
    assert_eq!(df.schema().fields().len(), 0); // columns count
    assert_eq!(df.clone().count().await?, 0); // rows count
    Ok(())
}
