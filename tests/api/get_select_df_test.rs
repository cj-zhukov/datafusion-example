use datafusion::assert_batches_eq;
use datafusion::prelude::*;
use rstest::rstest;

use datafusion_example::utils::utils::*;
use crate::helpers::*;

#[rstest]
#[case(get_df1().unwrap(), vec!["id", "name", "data"])]
#[case(get_df2().unwrap(), vec!["id", "name"])]
#[case(get_df3().unwrap(), vec!["id", "data"])]
fn test_get_column_names(#[case] df: DataFrame,#[case] expected: Vec<&str>) {
    assert_eq!(expected, get_column_names(df))
}

#[tokio::test]
async fn test_select_all_exclude() {
    let df = get_df1().unwrap();
    let res = select_all_exclude(df, &["data"]).unwrap();

    assert_eq!(res.schema().fields().len(), 2); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)]).unwrap();
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
        &rows.collect().await.unwrap()
    );
}
