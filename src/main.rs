use dev::*;

use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let now = Instant::now();

    let df = get_df().await?;
    df.show().await?;

    println!("end processing elapsed: {:.2?}", now.elapsed());

    Ok(())
}