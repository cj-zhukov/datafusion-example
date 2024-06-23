use dev::*;
use examples::*;

use std::time::Instant;

use anyhow::Result;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let now = Instant::now();

    let ctx = SessionContext::new();  

    let df1 = get_df().await?;
    let df2 = get_df2().await?.with_column_renamed("id", "id2")?;
    // df1.show().await?;
    // df2.show().await?;

    let df = df1
        .join(df2, JoinType::Inner, &["id"], &["id2"], None)?
        .select_columns(&["id", "name", "data"])?;
    let res = df_cols_to_json(ctx, df, &["name", "data"], Some("new_col")).await?;
    res.show().await?;

    // let res = add_pk_to_df(ctx, df, "pk").await?;
    // res.show().await?;

    // let cols = get_column_names(res);
    // println!("{:?}", cols);

    // let x = res.clone().select_columns(&["name"])?;
    // x.clone().show().await?;

    // let schema = x.schema().fields().to_owned();
    // println!("{:?}", schema);
    // let arrays = x.collect().await?;
    // let mut data = vec![];
    // for arr in arrays {
    //     let x = arr.columns();
    // }

    // let res = df_struct_example1().await?;
    // let res = df_cols_to_struct_test(ctx, res.clone()).await?;
    // let res = res.with_column("new_col", Expr::Literal(ScalarValue::Struct(arr.into())))?;
    // res.show().await?;

    // write_to_file(res, "data/foo.parquet").await?;

    println!("end processing elapsed: {:.2?}", now.elapsed());

    Ok(())
}