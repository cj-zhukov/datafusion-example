//! # These are core DataFrame API usage
//!
//! These examples demonstrate core DataFrame API usage.
//!
//! ## Usage
//! ```bash
//! cargo run --example dataframe -- [all|round_robin|1brc|least_values|round_robin|random]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//! - `1brc` — one billion row challenge example
//! - `least_values` — least values example with sql
//! - `round_robin` — round robin example with sql
//! - `random` — random example with sql

mod least_values;
mod one_billion_row_challenge;
mod random;
mod round_robin;

use color_eyre::{Report, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    LeastValues,
    OneBillionRowChallenge,
    Random,
    RoundRobin,
}

impl ExampleKind {
    const EXAMPLE_NAME: &str = "algo";

    fn runnable() -> impl Iterator<Item = ExampleKind> {
        ExampleKind::iter().filter(|v| !matches!(v, ExampleKind::All))
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::All => {
                for example in ExampleKind::runnable() {
                    println!("Running example: {example}");
                    Box::pin(example.run()).await?;
                }
            }
            ExampleKind::LeastValues => least_values::least_values_example().await?,
            ExampleKind::OneBillionRowChallenge => {
                one_billion_row_challenge::one_billion_row_challenge().await?
            }
            ExampleKind::Random => random::random_example().await?,
            ExampleKind::RoundRobin => round_robin::round_robin_example().await?,
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let usage = format!(
        "Usage: cargo run --example {} -- [{}]",
        ExampleKind::EXAMPLE_NAME,
        ExampleKind::VARIANTS.join("|")
    );

    let example: ExampleKind = std::env::args()
        .nth(1)
        .ok_or_else(|| Report::msg(format!("Missing argument. {usage}")))?
        .parse()
        .map_err(|_| Report::msg(format!("Unknown example. {usage}")))?;

    example.run().await
}
