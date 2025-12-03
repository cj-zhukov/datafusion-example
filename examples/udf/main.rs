//! # These are core DataFrame API usage
//!
//! These examples demonstrate core DataFrame API usage.
//!
//! ## Usage
//! ```bash
//! cargo run --example dataframe -- [all|udf]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//! - `udf` — user defined function examples

mod udf;

use std::str::FromStr;

use color_eyre::{Report, Result};

enum ExampleKind {
    All,
    Udf,
}

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::Udf => "udf",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = color_eyre::eyre::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "udf" => Ok(Self::Udf),
            _ => Err(Report::msg(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 2] = [Self::All, Self::Udf];

    const RUNNABLE_VARIANTS: [Self; 1] = [Self::Udf];

    const EXAMPLE_NAME: &str = "udf";

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
            ExampleKind::Udf => udf::udf_example().await?,
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
