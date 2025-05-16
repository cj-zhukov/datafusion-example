use color_eyre::Result;
use datafusion::assert_batches_eq;
use datafusion::prelude::*;
use rstest::rstest;

use crate::helpers::*;
use datafusion_example::utils::tools::*;

#[rstest]
#[case(get_df1()?, Some(vec!["id", "name", "data"]))]
#[case(get_df2()?, Some(vec!["id", "name"]))]
#[case(get_df3()?, Some(vec!["id", "data"]))]
#[case(get_empty_df()?, None)]
fn test_get_column_names(#[case] df: DataFrame, #[case] expected: Option<Vec<&str>>) -> Result<()> {
    assert_eq!(expected, get_column_names(&df));
    Ok(())
}

#[tokio::test]
async fn test_select_all_exclude() -> Result<()> {
    let df = get_df1()?;
    let res = select_all_exclude(df, &["data"])?;

    assert_eq!(res.schema().fields().len(), 2); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
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
        &rows.collect().await?
    );

    Ok(())
}
