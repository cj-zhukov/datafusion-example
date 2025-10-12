use color_eyre::Result;
use datafusion::arrow::array::{Float64Array, Int32Array, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::assert_batches_eq;
use datafusion::prelude::*;

use datafusion_example::utils::helpers::*;

#[tokio::test]
async fn test_add_pk_to_df() -> Result<()> {
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )?;

    let ctx = SessionContext::new();
    let res = add_pk_to_df(&ctx, df, "pk").await?;

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+----+",
            "| id | name | data | pk |",
            "+----+------+------+----+",
            "| 1  | foo  | 42   | 0  |",
            "| 2  | bar  | 43   | 1  |",
            "| 3  | baz  | 44   | 2  |",
            "+----+------+------+----+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_int_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"]
    )?;

    let data = vec![42, 43, 44];
    let res = add_int_col_to_df(&ctx, df, data, "data").await?;

    assert_eq!(res.schema().fields().len(), 3); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | name | data |",
            "+----+------+------+",
            "| 1  | foo  | 42   |",
            "| 2  | bar  | 43   |",
            "| 3  | baz  | 44   |",
            "+----+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_str_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "data" => [42, 43, 44]
    )?;

    let data = vec!["foo", "bar", "baz"];
    let res = add_str_col_to_df(&ctx, df, data, "name").await?;

    assert_eq!(res.schema().fields().len(), 3); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
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
async fn test_add_any_num_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "data" => [42, 43, 44]
    )?;

    let data = vec![1, 2, 3];
    let data_col = Int32Array::from(data);
    let df = add_any_num_col_to_df(&ctx, df, data_col, "col1").await?;

    let data = vec![1.1, 1.2, 1.3];
    let data_col = Float64Array::from(data);
    let res = add_any_num_col_to_df(&ctx, df, data_col, "col2").await?;

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+------+",
            "| id | data | col1 | col2 |",
            "+----+------+------+------+",
            "| 1  | 42   | 1    | 1.1  |",
            "| 2  | 43   | 2    | 1.2  |",
            "| 3  | 44   | 3    | 1.3  |",
            "+----+------+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_any_str_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "data" => [42, 43, 44]
    )?;

    let data = vec!["foo", "bar", "baz"];
    let data_col = StringArray::from(data);
    let df = add_any_str_col_to_df(&ctx, df, data_col, "col1").await?;

    let data = vec!["foo", "bar", "baz"];
    let data_col = LargeStringArray::from(data);
    let res = add_any_str_col_to_df(&ctx, df, data_col, "col2").await?;

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await?, 3); // rows count

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+------+",
            "| id | data | col1 | col2 |",
            "+----+------+------+------+",
            "| 1  | 42   | foo  | foo  |",
            "| 2  | 43   | bar  | bar  |",
            "| 3  | 44   | baz  | baz  |",
            "+----+------+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_col_arr_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = dataframe!(
        "id" => [1, 2, 3],
        "data" => [42, 43, 44]
    )?;

    let col1 = StringArray::from(vec!["foo", "bar", "baz"]);
    let col2 = Float64Array::from(vec![42.0, 43.0, 44.0]);
    let df = add_col_arr_to_df(&ctx, df, &col1, "col1").await?;
    let res = add_col_arr_to_df(&ctx, df, &col2, "col2").await?;

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+------+",
            "| id | data | col1 | col2 |",
            "+----+------+------+------+",
            "| 1  | 42   | foo  | 42.0 |",
            "| 2  | 43   | bar  | 43.0 |",
            "| 3  | 44   | baz  | 44.0 |",
            "+----+------+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_get_random_df() -> Result<()> {
    let ctx = SessionContext::new();
    let expected_types = vec![
        DataType::Int32,
        DataType::Float32,
        DataType::Utf8,
        DataType::Boolean,
    ];
    let df = get_random_df(&ctx, &expected_types, 5)?;

    assert_eq!(df.schema().fields().len(), 4);
    assert_eq!(df.clone().count().await?, 5);

    let schema = df.schema();
    for (i, expected_type) in expected_types.iter().enumerate() {
        let field = schema.field(i);
        assert_eq!(field.data_type(), expected_type);
    }

    Ok(())
}
