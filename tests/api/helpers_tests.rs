use color_eyre::Result;
use datafusion::arrow::array::{Float64Array, Int32Array, LargeStringArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::assert_batches_eq;
use datafusion::prelude::*;

use datafusion_example::utils::datasets::ExampleDataset;
use datafusion_example::utils::helpers::*;

#[tokio::test]
async fn test_add_pk_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;
    let res = add_pk_to_df(&ctx, df, "pk").await?;

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await?, 25); // rows count

    let rows = res.sort(vec![col("pk").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+----+",
            "| car   | speed | time                | pk |",
            "+-------+-------+---------------------+----+",
            "| red   | 20.0  | 1996-04-12T12:05:03 | 0  |",
            "| red   | 20.3  | 1996-04-12T12:05:04 | 1  |",
            "| red   | 21.4  | 1996-04-12T12:05:05 | 2  |",
            "| red   | 21.5  | 1996-04-12T12:05:06 | 3  |",
            "| red   | 19.0  | 1996-04-12T12:05:07 | 4  |",
            "| red   | 18.0  | 1996-04-12T12:05:08 | 5  |",
            "| red   | 17.0  | 1996-04-12T12:05:09 | 6  |",
            "| red   | 7.0   | 1996-04-12T12:05:10 | 7  |",
            "| red   | 7.1   | 1996-04-12T12:05:11 | 8  |",
            "| red   | 7.2   | 1996-04-12T12:05:12 | 9  |",
            "| red   | 3.0   | 1996-04-12T12:05:13 | 10 |",
            "| red   | 1.0   | 1996-04-12T12:05:14 | 11 |",
            "| red   | 0.0   | 1996-04-12T12:05:15 | 12 |",
            "| green | 10.0  | 1996-04-12T12:05:03 | 13 |",
            "| green | 10.3  | 1996-04-12T12:05:04 | 14 |",
            "| green | 10.4  | 1996-04-12T12:05:05 | 15 |",
            "| green | 10.5  | 1996-04-12T12:05:06 | 16 |",
            "| green | 11.0  | 1996-04-12T12:05:07 | 17 |",
            "| green | 12.0  | 1996-04-12T12:05:08 | 18 |",
            "| green | 14.0  | 1996-04-12T12:05:09 | 19 |",
            "| green | 15.0  | 1996-04-12T12:05:10 | 20 |",
            "| green | 15.1  | 1996-04-12T12:05:11 | 21 |",
            "| green | 15.2  | 1996-04-12T12:05:12 | 22 |",
            "| green | 8.0   | 1996-04-12T12:05:13 | 23 |",
            "| green | 2.0   | 1996-04-12T12:05:14 | 24 |",
            "+-------+-------+---------------------+----+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_int_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;

    let data = (0..25).step_by(1).collect::<Vec<i32>>();
    let res = add_int_col_to_df(&ctx, df, data, "data").await?;

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await?, 25); // rows count

    let rows = res.sort(vec![col("car").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+------+",
            "| car   | speed | time                | data |",
            "+-------+-------+---------------------+------+",
            "| green | 10.0  | 1996-04-12T12:05:03 | 13   |",
            "| green | 10.3  | 1996-04-12T12:05:04 | 14   |",
            "| green | 10.4  | 1996-04-12T12:05:05 | 15   |",
            "| green | 10.5  | 1996-04-12T12:05:06 | 16   |",
            "| green | 11.0  | 1996-04-12T12:05:07 | 17   |",
            "| green | 12.0  | 1996-04-12T12:05:08 | 18   |",
            "| green | 14.0  | 1996-04-12T12:05:09 | 19   |",
            "| green | 15.0  | 1996-04-12T12:05:10 | 20   |",
            "| green | 15.1  | 1996-04-12T12:05:11 | 21   |",
            "| green | 15.2  | 1996-04-12T12:05:12 | 22   |",
            "| green | 8.0   | 1996-04-12T12:05:13 | 23   |",
            "| green | 2.0   | 1996-04-12T12:05:14 | 24   |",
            "| red   | 20.0  | 1996-04-12T12:05:03 | 0    |",
            "| red   | 20.3  | 1996-04-12T12:05:04 | 1    |",
            "| red   | 21.4  | 1996-04-12T12:05:05 | 2    |",
            "| red   | 21.5  | 1996-04-12T12:05:06 | 3    |",
            "| red   | 19.0  | 1996-04-12T12:05:07 | 4    |",
            "| red   | 18.0  | 1996-04-12T12:05:08 | 5    |",
            "| red   | 17.0  | 1996-04-12T12:05:09 | 6    |",
            "| red   | 7.0   | 1996-04-12T12:05:10 | 7    |",
            "| red   | 7.1   | 1996-04-12T12:05:11 | 8    |",
            "| red   | 7.2   | 1996-04-12T12:05:12 | 9    |",
            "| red   | 3.0   | 1996-04-12T12:05:13 | 10   |",
            "| red   | 1.0   | 1996-04-12T12:05:14 | 11   |",
            "| red   | 0.0   | 1996-04-12T12:05:15 | 12   |",
            "+-------+-------+---------------------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_str_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;

    let data = vec!["foo"; 25];
    let res = add_str_col_to_df(&ctx, df, data, "name").await?;

    assert_eq!(res.schema().fields().len(), 4); // columns count
    assert_eq!(res.clone().count().await?, 25); // rows count

    let rows = res.sort(vec![col("car").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+------+",
            "| car   | speed | time                | name |",
            "+-------+-------+---------------------+------+",
            "| green | 10.0  | 1996-04-12T12:05:03 | foo  |",
            "| green | 10.3  | 1996-04-12T12:05:04 | foo  |",
            "| green | 10.4  | 1996-04-12T12:05:05 | foo  |",
            "| green | 10.5  | 1996-04-12T12:05:06 | foo  |",
            "| green | 11.0  | 1996-04-12T12:05:07 | foo  |",
            "| green | 12.0  | 1996-04-12T12:05:08 | foo  |",
            "| green | 14.0  | 1996-04-12T12:05:09 | foo  |",
            "| green | 15.0  | 1996-04-12T12:05:10 | foo  |",
            "| green | 15.1  | 1996-04-12T12:05:11 | foo  |",
            "| green | 15.2  | 1996-04-12T12:05:12 | foo  |",
            "| green | 8.0   | 1996-04-12T12:05:13 | foo  |",
            "| green | 2.0   | 1996-04-12T12:05:14 | foo  |",
            "| red   | 20.0  | 1996-04-12T12:05:03 | foo  |",
            "| red   | 20.3  | 1996-04-12T12:05:04 | foo  |",
            "| red   | 21.4  | 1996-04-12T12:05:05 | foo  |",
            "| red   | 21.5  | 1996-04-12T12:05:06 | foo  |",
            "| red   | 19.0  | 1996-04-12T12:05:07 | foo  |",
            "| red   | 18.0  | 1996-04-12T12:05:08 | foo  |",
            "| red   | 17.0  | 1996-04-12T12:05:09 | foo  |",
            "| red   | 7.0   | 1996-04-12T12:05:10 | foo  |",
            "| red   | 7.1   | 1996-04-12T12:05:11 | foo  |",
            "| red   | 7.2   | 1996-04-12T12:05:12 | foo  |",
            "| red   | 3.0   | 1996-04-12T12:05:13 | foo  |",
            "| red   | 1.0   | 1996-04-12T12:05:14 | foo  |",
            "| red   | 0.0   | 1996-04-12T12:05:15 | foo  |",
            "+-------+-------+---------------------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_any_num_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;

    let data = (0..25).step_by(1).collect::<Vec<i32>>();
    let data_col = Int32Array::from(data);
    let df = add_any_num_col_to_df(&ctx, df, data_col, "col1").await?;

    let data = (0..25).map(|x| x as f64).collect::<Vec<f64>>();
    let data_col = Float64Array::from(data);
    let res = add_any_num_col_to_df(&ctx, df, data_col, "col2").await?;

    assert_eq!(res.schema().fields().len(), 5); // columns count
    assert_eq!(res.clone().count().await?, 25); // rows count

    let rows = res.sort(vec![col("car").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+------+------+",
            "| car   | speed | time                | col1 | col2 |",
            "+-------+-------+---------------------+------+------+",
            "| green | 10.0  | 1996-04-12T12:05:03 | 13   | 13.0 |",
            "| green | 10.3  | 1996-04-12T12:05:04 | 14   | 14.0 |",
            "| green | 10.4  | 1996-04-12T12:05:05 | 15   | 15.0 |",
            "| green | 10.5  | 1996-04-12T12:05:06 | 16   | 16.0 |",
            "| green | 11.0  | 1996-04-12T12:05:07 | 17   | 17.0 |",
            "| green | 12.0  | 1996-04-12T12:05:08 | 18   | 18.0 |",
            "| green | 14.0  | 1996-04-12T12:05:09 | 19   | 19.0 |",
            "| green | 15.0  | 1996-04-12T12:05:10 | 20   | 20.0 |",
            "| green | 15.1  | 1996-04-12T12:05:11 | 21   | 21.0 |",
            "| green | 15.2  | 1996-04-12T12:05:12 | 22   | 22.0 |",
            "| green | 8.0   | 1996-04-12T12:05:13 | 23   | 23.0 |",
            "| green | 2.0   | 1996-04-12T12:05:14 | 24   | 24.0 |",
            "| red   | 20.0  | 1996-04-12T12:05:03 | 0    | 0.0  |",
            "| red   | 20.3  | 1996-04-12T12:05:04 | 1    | 1.0  |",
            "| red   | 21.4  | 1996-04-12T12:05:05 | 2    | 2.0  |",
            "| red   | 21.5  | 1996-04-12T12:05:06 | 3    | 3.0  |",
            "| red   | 19.0  | 1996-04-12T12:05:07 | 4    | 4.0  |",
            "| red   | 18.0  | 1996-04-12T12:05:08 | 5    | 5.0  |",
            "| red   | 17.0  | 1996-04-12T12:05:09 | 6    | 6.0  |",
            "| red   | 7.0   | 1996-04-12T12:05:10 | 7    | 7.0  |",
            "| red   | 7.1   | 1996-04-12T12:05:11 | 8    | 8.0  |",
            "| red   | 7.2   | 1996-04-12T12:05:12 | 9    | 9.0  |",
            "| red   | 3.0   | 1996-04-12T12:05:13 | 10   | 10.0 |",
            "| red   | 1.0   | 1996-04-12T12:05:14 | 11   | 11.0 |",
            "| red   | 0.0   | 1996-04-12T12:05:15 | 12   | 12.0 |",
            "+-------+-------+---------------------+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_any_str_col_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;
    
    let data = vec!["foo"; 25];
    let data_col = StringArray::from(data);
    let df = add_any_str_col_to_df(&ctx, df, data_col, "col1").await?;

    let data = vec!["bar"; 25];
    let data_col = LargeStringArray::from(data);
    let res = add_any_str_col_to_df(&ctx, df, data_col, "col2").await?;

    assert_eq!(res.schema().fields().len(), 5); // columns count
    assert_eq!(res.clone().count().await?, 25); // rows count

    let rows = res.sort(vec![col("car").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+------+------+",
            "| car   | speed | time                | col1 | col2 |",
            "+-------+-------+---------------------+------+------+",
            "| green | 10.0  | 1996-04-12T12:05:03 | foo  | bar  |",
            "| green | 10.3  | 1996-04-12T12:05:04 | foo  | bar  |",
            "| green | 10.4  | 1996-04-12T12:05:05 | foo  | bar  |",
            "| green | 10.5  | 1996-04-12T12:05:06 | foo  | bar  |",
            "| green | 11.0  | 1996-04-12T12:05:07 | foo  | bar  |",
            "| green | 12.0  | 1996-04-12T12:05:08 | foo  | bar  |",
            "| green | 14.0  | 1996-04-12T12:05:09 | foo  | bar  |",
            "| green | 15.0  | 1996-04-12T12:05:10 | foo  | bar  |",
            "| green | 15.1  | 1996-04-12T12:05:11 | foo  | bar  |",
            "| green | 15.2  | 1996-04-12T12:05:12 | foo  | bar  |",
            "| green | 8.0   | 1996-04-12T12:05:13 | foo  | bar  |",
            "| green | 2.0   | 1996-04-12T12:05:14 | foo  | bar  |",
            "| red   | 20.0  | 1996-04-12T12:05:03 | foo  | bar  |",
            "| red   | 20.3  | 1996-04-12T12:05:04 | foo  | bar  |",
            "| red   | 21.4  | 1996-04-12T12:05:05 | foo  | bar  |",
            "| red   | 21.5  | 1996-04-12T12:05:06 | foo  | bar  |",
            "| red   | 19.0  | 1996-04-12T12:05:07 | foo  | bar  |",
            "| red   | 18.0  | 1996-04-12T12:05:08 | foo  | bar  |",
            "| red   | 17.0  | 1996-04-12T12:05:09 | foo  | bar  |",
            "| red   | 7.0   | 1996-04-12T12:05:10 | foo  | bar  |",
            "| red   | 7.1   | 1996-04-12T12:05:11 | foo  | bar  |",
            "| red   | 7.2   | 1996-04-12T12:05:12 | foo  | bar  |",
            "| red   | 3.0   | 1996-04-12T12:05:13 | foo  | bar  |",
            "| red   | 1.0   | 1996-04-12T12:05:14 | foo  | bar  |",
            "| red   | 0.0   | 1996-04-12T12:05:15 | foo  | bar  |",
            "+-------+-------+---------------------+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_add_col_arr_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let cars = ExampleDataset::Cars;
    let df = cars.dataframe(&ctx).await?;

    let data = (0..25).step_by(1).collect::<Vec<i32>>();
    let col1 = Int32Array::from(data);
    let data = vec!["foo"; 25];
    let col2 = StringArray::from(data);
    let df = add_col_arr_to_df(&ctx, df, &col1, "col1").await?;
    let res = add_col_arr_to_df(&ctx, df, &col2, "col2").await?;

    let rows = res.sort(vec![col("car").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+-------+-------+---------------------+------+------+",
            "| car   | speed | time                | col1 | col2 |",
            "+-------+-------+---------------------+------+------+",
            "| green | 10.0  | 1996-04-12T12:05:03 | 13   | foo  |",
            "| green | 10.3  | 1996-04-12T12:05:04 | 14   | foo  |",
            "| green | 10.4  | 1996-04-12T12:05:05 | 15   | foo  |",
            "| green | 10.5  | 1996-04-12T12:05:06 | 16   | foo  |",
            "| green | 11.0  | 1996-04-12T12:05:07 | 17   | foo  |",
            "| green | 12.0  | 1996-04-12T12:05:08 | 18   | foo  |",
            "| green | 14.0  | 1996-04-12T12:05:09 | 19   | foo  |",
            "| green | 15.0  | 1996-04-12T12:05:10 | 20   | foo  |",
            "| green | 15.1  | 1996-04-12T12:05:11 | 21   | foo  |",
            "| green | 15.2  | 1996-04-12T12:05:12 | 22   | foo  |",
            "| green | 8.0   | 1996-04-12T12:05:13 | 23   | foo  |",
            "| green | 2.0   | 1996-04-12T12:05:14 | 24   | foo  |",
            "| red   | 20.0  | 1996-04-12T12:05:03 | 0    | foo  |",
            "| red   | 20.3  | 1996-04-12T12:05:04 | 1    | foo  |",
            "| red   | 21.4  | 1996-04-12T12:05:05 | 2    | foo  |",
            "| red   | 21.5  | 1996-04-12T12:05:06 | 3    | foo  |",
            "| red   | 19.0  | 1996-04-12T12:05:07 | 4    | foo  |",
            "| red   | 18.0  | 1996-04-12T12:05:08 | 5    | foo  |",
            "| red   | 17.0  | 1996-04-12T12:05:09 | 6    | foo  |",
            "| red   | 7.0   | 1996-04-12T12:05:10 | 7    | foo  |",
            "| red   | 7.1   | 1996-04-12T12:05:11 | 8    | foo  |",
            "| red   | 7.2   | 1996-04-12T12:05:12 | 9    | foo  |",
            "| red   | 3.0   | 1996-04-12T12:05:13 | 10   | foo  |",
            "| red   | 1.0   | 1996-04-12T12:05:14 | 11   | foo  |",
            "| red   | 0.0   | 1996-04-12T12:05:15 | 12   | foo  |",
            "+-------+-------+---------------------+------+------+",
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
        let col_name = format!("column_{}", i + 1);
        let field = schema.field_with_name(None, &col_name)?;
        assert_eq!(field.data_type(), expected_type);
    }

    Ok(())
}
