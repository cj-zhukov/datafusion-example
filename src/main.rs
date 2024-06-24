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
    df.clone().show().await?;

    // let res = df_cols_to_json(ctx.clone(), df, &["name", "data"], Some("new_col")).await?;
    let res = df_cols_to_struct(ctx, df, &["name", "data"], Some("new_col")).await?;
    res.show().await?;

    // let cols = get_column_names(res);
    // println!("{:?}", cols);

    // write_to_file(res, "data/foo.parquet").await?;

    println!("end processing elapsed: {:.2?}", now.elapsed());

    Ok(())
}