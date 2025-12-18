# DataFusion Examples
DataFusion APIs code examples

## Running Examples
To run an example, use the `cargo run` command, such as:

```bash
# Change to the examples directory
cd examples

# Run all examples in a group
cargo run --example <group> -- all

# Run a specific example within a group
cargo run --example <group> -- <subcommand>

# Run all examples in the `dataframe` group
cargo run --example dataframe -- all

# Run a single example from the `dataframe` group
# (apply the same pattern for any other group)
cargo run --example dataframe -- dataframe
```

### Group: algo
| Subcommand                  | File Path                                                                | Description                                 |
| --------------------------- | ------------------------------------------------------------------------ | ------------------------------------------- |
| `least_values`              | [`algo/least_values.rs`](algo/least_values.rs)                           | Finding the least N values using DataFusion |
| `one_billion_row_challenge` | [`algo/one_billion_row_challenge.rs`](algo/one_billion_row_challenge.rs) | Large-scale performance benchmark example   |
| `random`                    | [`algo/random.rs`](algo/random.rs)                                       | Generating random data and processing it    |
| `round_robin`               | [`algo/round_robin.rs`](algo/round_robin.rs)                             | Round-robin data distribution algorithm     |

### Group: dataframe
| Subcommand                     | File Path                                                                                         | Description                                     |
| ------------------------------ | ------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| `create`                       | [`dataframe/create.rs`](dataframe/create.rs)                                             | Creating DataFrames from Arrow arrays           |
| `query`                        | [`dataframe/query.rs`](dataframe/query.rs)                                               | Running SQL queries on DataFrames               |
| `operations`                   | [`dataframe/operations.rs`](dataframe/operations.rs)                                     | Joins, add/update column, sort, filter          |
| `actions`                      | [`dataframe/actions.rs`](dataframe/actions.rs)                                           | Converting columns to JSON, struct, projections |
| `deserialize_to_struct`        | [`dataframe/deserialize_to_struct.rs`](dataframe/deserialize_to_struct.rs)               | Mapping DataFrame rows into Rust structs        |
| `convert_vec_structs_to_df`    | [`dataframe/convert_vec_structs_to_df.rs`](dataframe/convert_vec_structs_to_df.rs)       | Converting Rust structs to DataFrames           |
| `convert_vec_structs_to_df_v2` | [`dataframe/convert_vec_structs_to_df_v2.rs`](dataframe/convert_vec_structs_to_df_v2.rs) | Improved struct-to-DataFrame conversion         |
| `convert_vec_structs_to_df_v3` | [`dataframe/convert_vec_structs_to_df_v3.rs`](dataframe/convert_vec_structs_to_df_v3.rs) | Struct-to-DataFrame conversion using traits     |

### Group: udf
| Subcommand | File Path                  | Description                                     |
| ---------- | -------------------------- | ----------------------------------------------- |
| `udf`      | [`udf/udf.rs`](udf/udf.rs) | Defining and using User-Defined Functions in DF |
