use dev::*;

use std::time::Instant;

use datafusion::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let now = Instant::now();

    // let ctx = SessionContext::new();
    // let df1 = get_df().await?;
    // let df2 = get_df2().await?;
    // df1.show().await?;
    // df2.show().await?;

    // let res = df1.join(df2, JoinType::Inner, &["id"], &["id"], None)?;
    // res.show().await?;
    // write_to_file(res, "data.foo.parquet").await?;

    println!("end processing elapsed: {:.2?}", now.elapsed());

    Ok(())
}
