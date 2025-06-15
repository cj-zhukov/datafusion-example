use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, GenericByteArray,
    Int32Array, Int64Array, PrimitiveArray, StringArray,
};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, ByteArrayType, DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::{error::DataFusionError, prelude::*};

use crate::{error::UtilsError, utils::dataframe::concat_arrays};

/// Add auto-increment column to dataframe
/// # Examples
/// ```
/// use datafusion::prelude::*;
/// # use color_eyre::Result;
/// # use datafusion_example::utils::helpers::add_pk_to_df;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = dataframe!(
///     "id" => [1, 2, 3],
///     "name" => ["foo", "bar", "baz"]
/// )?;
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// let ctx = SessionContext::new();
/// let res = add_pk_to_df(&ctx, df, "pk").await?;
/// // +----+------+----+
/// // | id | name | pk |
/// // +----+------+----+
/// // | 1  | foo  | 0  |
/// // | 2  | bar  | 1  |
/// // | 3  | baz  | 2  |
/// // +----+------+----+
/// # Ok(())
/// # }
/// ```
pub async fn add_pk_to_df(
    ctx: &SessionContext,
    df: DataFrame,
    col_name: &str,
) -> Result<DataFrame, UtilsError> {
    let schema = df.schema().as_arrow().clone();
    let mut arrays = concat_arrays(df).await?;

    let first_array = arrays.first().ok_or_else(|| {
        DataFusionError::Execution("Cannot add PK to empty DataFrame".to_string())
    })?;
    let max_len = first_array.len();
    debug_assert!(max_len <= i32::MAX as usize);

    let pk_array: ArrayRef = Arc::new(Int32Array::from_iter(0..max_len as i32));
    arrays.push(pk_array);

    let mut new_fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    new_fields.push(Field::new(col_name, DataType::Int32, false));
    let new_schema = Arc::new(Schema::new(new_fields));

    let batch = RecordBatch::try_new(new_schema, arrays)?;
    let res = ctx.read_batch(batch)?;
    Ok(res)
}

/// Add int32 column to existing dataframe
pub async fn add_int_col_to_df(
    ctx: &SessionContext,
    df: DataFrame,
    data: Vec<i32>,
    col_name: &str,
) -> Result<DataFrame, UtilsError> {
    let schema = df.schema().as_arrow().clone();
    let mut arrays = concat_arrays(df).await?;
    let new_col: ArrayRef = Arc::new(Int32Array::from(data));
    arrays.push(new_col);
    let schema_new_col = Schema::new(vec![Field::new(col_name, DataType::Int32, true)]);
    let schema_new = Schema::try_merge(vec![schema, schema_new_col])?;
    let batch = RecordBatch::try_new(Arc::new(schema_new), arrays)?;
    let res = ctx.read_batch(batch)?;
    Ok(res)
}

/// Add string column to existing dataframe
pub async fn add_str_col_to_df(
    ctx: &SessionContext,
    df: DataFrame,
    data: Vec<&str>,
    col_name: &str,
) -> Result<DataFrame, UtilsError> {
    let schema = df.schema().as_arrow().clone();
    let mut arrays = concat_arrays(df).await?;
    let new_col: ArrayRef = Arc::new(StringArray::from(data));
    arrays.push(new_col);
    let schema_new_col = Schema::new(vec![Field::new(col_name, DataType::Utf8, true)]);
    let schema_new = Schema::try_merge(vec![schema, schema_new_col])?;
    let batch = RecordBatch::try_new(Arc::new(schema_new), arrays)?;
    let res = ctx.read_batch(batch)?;
    Ok(res)
}

/// Add any numeric column to existing dataframe
pub async fn add_any_num_col_to_df<T>(
    ctx: &SessionContext,
    df: DataFrame,
    data: PrimitiveArray<T>,
    col_name: &str,
) -> Result<DataFrame, UtilsError>
where
    T: ArrowPrimitiveType,
{
    let schema = df.schema().as_arrow().clone();
    let mut arrays = concat_arrays(df).await?;
    let schema_new_col = Schema::new(vec![Field::new(col_name, data.data_type().clone(), true)]);
    let new_col: ArrayRef = Arc::new(data);
    arrays.push(new_col);
    let schema_new = Schema::try_merge(vec![schema, schema_new_col])?;
    let batch = RecordBatch::try_new(Arc::new(schema_new), arrays)?;
    let res = ctx.read_batch(batch)?;
    Ok(res)
}

/// Add any string column to existing dataframe
pub async fn add_any_str_col_to_df<T>(
    ctx: &SessionContext,
    df: DataFrame,
    data: GenericByteArray<T>,
    col_name: &str,
) -> Result<DataFrame, UtilsError>
where
    T: ByteArrayType,
{
    let schema = df.schema().as_arrow().clone();
    let mut arrays = concat_arrays(df).await?;
    let schema_new_col = Schema::new(vec![Field::new(col_name, data.data_type().clone(), true)]);
    let new_col: ArrayRef = Arc::new(data);
    arrays.push(new_col);
    let schema_new = Schema::try_merge(vec![schema, schema_new_col])?;
    let batch = RecordBatch::try_new(Arc::new(schema_new), arrays)?;
    let res = ctx.read_batch(batch)?;
    Ok(res)
}

/// Add column to existing dataframe
/// # Examples
/// ```
/// use std::sync::Arc;
/// # use color_eyre::Result;
/// use datafusion::prelude::*;
/// use datafusion::arrow::array::StringArray;
/// # use datafusion_example::utils::helpers::add_col_to_df;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = dataframe!("id" => [1, 2, 3])?;
/// let name = StringArray::from(vec!["foo", "bar", "baz"]);
/// let ctx = SessionContext::new();
/// let res = add_col_to_df(&ctx, df, Arc::new(name), "name").await?;
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// # Ok(())
/// # }
/// ```
pub async fn add_col_to_df(
    ctx: &SessionContext,
    df: DataFrame,
    data: ArrayRef,
    col_name: &str,
) -> Result<DataFrame, UtilsError> {
    let schema_ref = Arc::new(df.schema().as_arrow().clone());
    let mut arrays = concat_arrays(df).await?;
    let row_count = arrays
        .first()
        .ok_or_else(|| DataFusionError::Execution("empty DataFrame".into()))?
        .len();

    if data.len() != row_count {
        return Err(DataFusionError::Execution(format!(
            "new column '{col_name}' has length {}, expected {row_count}",
            data.len()
        ))
        .into());
    }

    arrays.push(data);

    let mut new_fields: Vec<Field> = schema_ref
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let new_col_type = arrays
        .last()
        .expect("arrays should contain at least one column")
        .data_type()
        .clone();
    new_fields.push(Field::new(col_name, new_col_type, true));
    let new_schema = Arc::new(Schema::new(new_fields));

    let final_batch = RecordBatch::try_new(new_schema, arrays)?;
    let res = ctx.read_batch(final_batch)?;
    Ok(res)
}

/// Add column to existing dataframe
/// # Examples
/// ```
/// # use color_eyre::Result;
/// use datafusion::prelude::*;
/// use datafusion::arrow::array::StringArray;
/// # use datafusion_example::utils::helpers::add_col_arr_to_df;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = dataframe!("id" => [1, 2, 3])?;
/// let name = StringArray::from(vec!["foo", "bar", "baz"]);
/// let ctx = SessionContext::new();
/// let res = add_col_arr_to_df(&ctx, df, &name, "name").await?;
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// # Ok(())
/// # }
/// ```
pub async fn add_col_arr_to_df(
    ctx: &SessionContext,
    df: DataFrame,
    data: &dyn Array,
    col_name: &str,
) -> Result<DataFrame, UtilsError> {
    let schema = df.schema().as_arrow().clone();
    let mut arrays = concat_arrays(df).await?;
    let schema_new_col = Schema::new(vec![Field::new(col_name, data.data_type().clone(), true)]);

    let array: Arc<dyn Array> = match data.data_type() {
        DataType::Utf8 => {
            let array: &StringArray = data.as_any().downcast_ref().unwrap();
            Arc::new(array.to_owned())
        }
        DataType::Int32 => {
            let array: &Int32Array = data.as_any().downcast_ref().unwrap();
            Arc::new(array.to_owned())
        }
        DataType::Int64 => {
            let array: &Int64Array = data.as_any().downcast_ref().unwrap();
            Arc::new(array.to_owned())
        }
        DataType::Float32 => {
            let array: &Float32Array = data.as_any().downcast_ref().unwrap();
            Arc::new(array.to_owned())
        }
        DataType::Float64 => {
            let array: &Float64Array = data.as_any().downcast_ref().unwrap();
            Arc::new(array.to_owned())
        }
        DataType::Binary => {
            let array: &BinaryArray = data.as_any().downcast_ref().unwrap();
            Arc::new(array.to_owned())
        }
        DataType::Boolean => {
            let array: &BooleanArray = data.as_any().downcast_ref().unwrap();
            Arc::new(array.to_owned())
        }
        _ => unimplemented!(),
    };

    arrays.push(array);
    let schema_new = Schema::try_merge(vec![schema, schema_new_col])?;
    let batch = RecordBatch::try_new(Arc::new(schema_new), arrays)?;
    let res = ctx.read_batch(batch)?;
    Ok(res)
}
