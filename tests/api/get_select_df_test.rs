use datafusion::assert_batches_eq;
use datafusion::prelude::*;

use datafusion_example::utils::utils::*;
use crate::helpers::*;

#[tokio::test]
async fn test_get_column_names() {
    let df = get_df1().await.unwrap();
    let cols = get_column_names(df);
    assert_eq!(cols, vec!["id", "name", "data"]);
}

#[tokio::test]
async fn test_select_all_exclude() {
    let df = get_df1().await.unwrap();
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
