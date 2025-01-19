use color_eyre::Result;
use datafusion::assert_batches_eq;
use datafusion::prelude::*;
use rstest::rstest;

use crate::helpers::*;
use datafusion_example::utils::utils::*;

#[rstest]
#[case(get_df1()?, vec!["id", "name", "data"])]
#[case(get_df2()?, vec!["id", "name"])]
#[case(get_df3()?, vec!["id", "data"])]
fn test_get_column_names(#[case] df: DataFrame, #[case] expected: Vec<&str>) -> Result<()> {
    assert_eq!(expected, get_column_names(df));
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
