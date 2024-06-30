use dev::*;
use examples::*;

use std::time::Instant;

use anyhow::Result;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    let now = Instant::now();

    let ctx = SessionContext::new(); 
    // let df = ctx.read_parquet("data/foo.parquet", ParquetReadOptions::default()).await?;
    // let df1 = get_df()?;
    // let df2 = get_df()?;
    // df1.show().await?;
    // df2.show().await?;
    // let df2 = get_df2().await?.with_column_renamed("id", "id2")?;
    // let res = concat_dfs(ctx, vec![df1, df2]).await?;
    // res.show().await?;

    // let res = df1
    //     .join(df2, JoinType::Inner, &["id"], &["id2"], None)?
    //     .select_columns(&["id", "name", "data"])?;
    // res.show().await?;

    // let res = df_cols_to_json(ctx.clone(), df, &["name", "data"], Some("new_col")).await?;
    // let res = df_cols_to_struct(ctx, df, &["name", "data"], Some("new_col")).await?;
    // res.show().await?;


    // write_df_to_file(res, "data/foo_df.parquet").await?;

    println!("end processing elapsed: {:.2?}", now.elapsed());

    Ok(())
}