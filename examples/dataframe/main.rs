//! # These are core DataFrame API usage
//!
//! These examples demonstrate core DataFrame API usage.
//!
//! ## Usage
//! ```bash
//! cargo run --example dataframe -- [all|actions|convert_vec_structs_to_df|convert_vec_structs_to_df_v2|convert_vec_structs_to_df_v3|create|deserialize_to_struct|operations|query]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//! - `actions` — examples of actions with dataframe like converting columns to json, struct, etc
//! - `convert_vec_structs_to_df` — example of deserialization vec of custom structs to dataframe
//! - `convert_vec_structs_to_df_v2` — example of deserialization vec of custom structs to dataframe
//! - `convert_vec_structs_to_df_v3` — example of deserialization vec of custom structs to dataframe using trait
//! - `create` — Creating dataframe examples
//! - `deserialize_to_struct` — example of deserialization dataframe to struct
//! - `operations` — examples of operations with dataframe like joins, add/update column, etc
//! - `query` — quering dataframe examples

mod actions;
mod convert_vec_structs_to_df;
mod convert_vec_structs_to_df_v2;
mod convert_vec_structs_to_df_v3;
mod create;
mod deserialize_to_struct;
mod operations;
mod query;

use std::str::FromStr;

use color_eyre::{Report, Result};

enum ExampleKind {
    All,
    Actions,
    ConvertVecStructsToDf,
    ConvertVecStructsToDfV2,
    ConvertVecStructsToDfV3,
    Create,
    DeserializeToStruct,
    Operations,
    Query,
}

impl AsRef<str> for ExampleKind {
    fn as_ref(&self) -> &str {
        match self {
            Self::All => "all",
            Self::Actions => "actions",
            Self::ConvertVecStructsToDf => "convert_vec_structs_to_df",
            Self::ConvertVecStructsToDfV2 => "convert_vec_structs_to_df_v2",
            Self::ConvertVecStructsToDfV3 => "convert_vec_structs_to_df_v3",
            Self::Create => "create",
            Self::DeserializeToStruct => "deserialize_to_struct",
            Self::Operations => "operations",
            Self::Query => "query",
        }
    }
}

impl FromStr for ExampleKind {
    type Err = color_eyre::eyre::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "actions" => Ok(Self::Actions),
            "convert_vec_structs_to_df" => Ok(Self::ConvertVecStructsToDf),
            "convert_vec_structs_to_df_v2" => Ok(Self::ConvertVecStructsToDfV2),
            "convert_vec_structs_to_df_v3" => Ok(Self::ConvertVecStructsToDfV3),
            "create" => Ok(Self::Create),
            "deserialize_to_struct" => Ok(Self::DeserializeToStruct),
            "operations" => Ok(Self::Operations),
            "query" => Ok(Self::Query),
            _ => Err(Report::msg(format!("Unknown example: {s}"))),
        }
    }
}

impl ExampleKind {
    const ALL_VARIANTS: [Self; 9] = [
        Self::All,
        Self::Actions,
        Self::ConvertVecStructsToDf,
        Self::ConvertVecStructsToDfV2,
        Self::ConvertVecStructsToDfV3,
        Self::Create,
        Self::DeserializeToStruct,
        Self::Operations,
        Self::Query,
    ];

    const RUNNABLE_VARIANTS: [Self; 8] = [
        Self::Actions,
        Self::ConvertVecStructsToDf,
        Self::ConvertVecStructsToDfV2,
        Self::ConvertVecStructsToDfV3,
        Self::Create,
        Self::DeserializeToStruct,
        Self::Operations,
        Self::Query,
    ];

    const EXAMPLE_NAME: &str = "dataframe";

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
            ExampleKind::Actions => actions::actions_example().await?,
            ExampleKind::ConvertVecStructsToDf => {
                convert_vec_structs_to_df::convert_vec_structs_to_df_example().await?
            }
            ExampleKind::ConvertVecStructsToDfV2 => {
                convert_vec_structs_to_df_v2::convert_vec_structs_to_df_v2_example().await?
            }
            ExampleKind::ConvertVecStructsToDfV3 => {
                convert_vec_structs_to_df_v3::convert_vec_structs_to_df_v3_example().await?
            }
            ExampleKind::Create => create::create_example().await?,
            ExampleKind::DeserializeToStruct => {
                deserialize_to_struct::deserialize_to_struct_examle().await?
            }
            ExampleKind::Operations => operations::operations_example().await?,
            ExampleKind::Query => query::query_example().await?,
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
