use dev::*;

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

    let res = df1.join(df2, JoinType::Inner, &["id"], &["id2"], None)?.select_columns(&["id", "name", "data"])?;
    let res = df_cols_to_json(ctx.clone(), res, &["name", "data"], "id", Some("metadata"), Some(false)).await?;
    res.show().await?;

    // let df = df_struct_example1().await?;
    // df.show().await?;

    // write_to_file(res, "data/foo.parquet").await?;

    // let res = df_cols_to_struct(ctx, res, &["name", "data"]).await?;

    println!("end processing elapsed: {:.2?}", now.elapsed());

    Ok(())
}
