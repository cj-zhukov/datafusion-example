use std::sync::Arc;

use color_eyre::Result;
use datafusion::{
    arrow::{
        array::{Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    },
    prelude::*,
};

/// # Examples
/// ```
/// # use color_eyre::Result;
/// # fn main() -> Result<()> {
/// let df = get_df1()?;
/// // +----+------+------+
/// // | id | name | data |
/// // +----+------+------+
/// // | 1  | foo  | 42   |
/// // | 2  | bar  | 43   |
/// // | 3  | baz  | 44   |
/// // +----+------+------+
/// # Ok(())
/// # }
/// ```     
pub fn get_df1() -> Result<DataFrame> {
    let ctx = SessionContext::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;

    Ok(ctx.read_batch(batch)?)
}

/// # Examples
/// ```
/// # use color_eyre::Result;
/// # fn main() -> Result<()> {
/// let df = get_df2()?;
/// // +----+------+
/// // | id | name |
/// // +----+------+
/// // | 1  | foo  |
/// // | 2  | bar  |
/// // | 3  | baz  |
/// // +----+------+
/// # Ok(())
/// # }
/// ```
pub fn get_df2() -> Result<DataFrame> {
    let ctx = SessionContext::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;

    Ok(ctx.read_batch(batch)?)
}

/// # Examples
/// ```
/// # use color_eyre::Result;
/// # fn main() -> Result<()> {
/// let df = get_df3()?;
/// // +----+-----+
/// // | id | data|
/// // +----+-----+
/// // | 1  | 42  |
/// // | 2  | 43  |
/// // | 3  | 44  |
/// // +----+-----+
/// # Ok(())
/// # }
/// ```
pub fn get_df3() -> Result<DataFrame> {
    let ctx = SessionContext::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;

    Ok(ctx.read_batch(batch)?)
}

/// Get empty dataframe
pub fn get_empty_df() -> Result<DataFrame> {
    let ctx = SessionContext::new();
    let df = ctx.read_empty()?;
    Ok(df)
}

/// # Examples
/// ```
/// # use color_eyre::Result;
/// # fn main() -> Result<()> {
/// let df = get_schema()?;
/// // id Int32
/// // name Utf8
/// // data Int32
/// # Ok(())
/// # }
/// ```
pub fn get_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ])
}
