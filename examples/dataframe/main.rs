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

use color_eyre::{Report, Result};
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
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

impl ExampleKind {
    const EXAMPLE_NAME: &str = "dataframe";

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
        ExampleKind::VARIANTS.join("|")
    );

    let example: ExampleKind = std::env::args()
        .nth(1)
        .ok_or_else(|| Report::msg(format!("Missing argument. {usage}")))?
        .parse()
        .map_err(|_| Report::msg(format!("Unknown example. {usage}")))?;

    example.run().await
}
