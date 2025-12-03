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

use std::str::FromStr;

use color_eyre::{Report, Result};

enum ExampleKind {
    All,
    LeastValues,
    OneBillionRowChallenge,
    Random,
    RoundRobin,
}

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::LeastValues => "least_values",
            Self::OneBillionRowChallenge => "1brc",
            Self::Random => "random",
            Self::RoundRobin => "round_robin",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = color_eyre::eyre::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "least_values" => Ok(Self::LeastValues),
            "1brc" => Ok(Self::OneBillionRowChallenge),
            "random" => Ok(Self::Random),
            "round_robin" => Ok(Self::RoundRobin),
            _ => Err(Report::msg(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 5] = [
        Self::All,
        Self::LeastValues,
        Self::OneBillionRowChallenge,
        Self::Random,
        Self::RoundRobin,
    ];

    const RUNNABLE_VARIANTS: [Self; 4] = [
        Self::LeastValues,
        Self::OneBillionRowChallenge,
        Self::Random,
        Self::RoundRobin,
    ];

    const EXAMPLE_NAME: &str = "algo";

    fn variants() -> Vec<&'static str> {
        Self::ALL_VARIANTS
            .iter()
            .map(|example| example.as_ref())
            .collect()
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::All => {
                for example in ExampleKind::RUNNABLE_VARIANTS {
                    println!("Running example: {}", example.as_ref());
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
        ExampleKind::variants().join("|")
    );

    let arg = std::env::args().nth(1).ok_or_else(|| {
        eprintln!("{usage}");
        Report::msg("Missing argument".to_string())
    })?;

    let example = arg.parse::<ExampleKind>()?;
    example.run().await
}
