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

/// Select dataframe with all columns except to_exclude (better use drop_columns)
/// # Examples
/// ```
/// use datafusion::prelude::*;
/// # use datafusion_example::utils::helpers::select_all_exclude;
/// let df = dataframe!(
///     "id" => [1, 2, 3],
///     "name" => ["foo", "bar", "baz"],
///     "data" => [42, 43, 44]
/// ).unwrap();
/// // +----+------+------+
/// // | id | name | data |
/// // +----+------+------+
/// // | 1  | foo  | 42   |
/// // | 2  | bar  | 43   |
/// // | 3  | baz  | 44   |
/// // +----+------+------+
/// let ctx = SessionContext::new();
/// let res = select_all_exclude(df, &["name", "data"]);
/// // +----+
/// // | id |
/// // +----+
/// // | 1  |
/// // | 2  |
/// // | 3  |
/// // +----+
/// ```
pub fn select_all_exclude(df: DataFrame, to_exclude: &[&str]) -> Result<DataFrame, UtilsError> {
    let schema = df.schema().clone();
    let columns = schema
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .filter(|name| !to_exclude.contains(name))
        .collect::<Vec<_>>();
    let res = df.select_columns(&columns)?;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::dataframe::{concat_df_batches, get_column_names};

    use color_eyre::Result;
    use datafusion::arrow::datatypes::{Int32Type, Utf8Type};
    use datafusion::arrow::compute::concat_batches;
    use rstest::rstest;

    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id"], Some(vec!["name", "data"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["name"], Some(vec!["id", "data"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["data"], Some(vec!["id", "name"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "name"], Some(vec!["data"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "data"], Some(vec!["name"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["name", "data"], Some(vec!["id"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "name", "data"], None)]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["foo"], Some(vec!["id", "name", "data"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &[""], Some(vec!["id", "name", "data"]))]
    #[case(dataframe!()?, &["id", "name", "data"], None)]
    fn test_select_all_exclude(
        #[case] df: DataFrame,
        #[case] to_exclude: &[&str],
        #[case] expected: Option<Vec<&str>>,
    ) -> Result<()> {
        let df = select_all_exclude(df, to_exclude)?;
        let res = get_column_names(&df);
        assert_eq!(expected, res);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3])?, vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef])]
    #[case(dataframe!("name" => ["foo", "bar", "baz"])?, vec![Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef])]
    async fn test_add_pk_to_df(
        #[case] df: DataFrame,
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let df = add_pk_to_df(&ctx, df, "pk").await?;
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;
        let batch = concat_batches(&Arc::new(schema), &batches)?;
        let arrays: Vec<ArrayRef> = batch.columns().to_vec();
        assert_eq!(arrays, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_add_pk_to_df_err() -> Result<()> {
        let ctx = SessionContext::new();
        let df = dataframe!()?;
        let result = add_pk_to_df(&ctx, df, "pk").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        dbg!(err_msg.clone());
        assert!(err_msg.contains("DataFusionError"), "Empty DataFrame");
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3])?, vec![1, 2, 3], vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef])]
    #[case(dataframe!("name" => ["foo", "bar", "baz"])?, vec![1, 2, 3], vec![Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef])]
    #[case(dataframe!("data" => [true, true, false])?, vec![1, 2, 3], vec![Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef, Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef])]
    async fn test_add_int_col_to_df(
        #[case] df: DataFrame,
        #[case] data: Vec<i32>,
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let res = add_int_col_to_df(&ctx, df, data, "new_col").await?;
        let batch = concat_df_batches(res).await?;
        let arrays: Vec<ArrayRef> = batch.columns().to_vec();
        assert_eq!(arrays, expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3])?, vec!["foo", "bar", "baz"], vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    #[case(dataframe!("name" => ["foo", "bar", "baz"])?, vec!["foo", "bar", "baz"], vec![Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    #[case(dataframe!("data" => [true, true, false])?, vec!["foo", "bar", "baz"], vec![Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    async fn test_add_str_col_to_df(
        #[case] df: DataFrame,
        #[case] data: Vec<&str>,
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let res = add_str_col_to_df(&ctx, df, data, "new_col").await?;
        let batch = concat_df_batches(res).await?;
        let arrays: Vec<ArrayRef> = batch.columns().to_vec();
        assert_eq!(arrays, expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3])?, Into::<PrimitiveArray<Int32Type>>::into(vec![1, 2, 3]), vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef])]
    #[case(dataframe!("name" => ["foo", "bar", "baz"])?, Into::<PrimitiveArray<Int32Type>>::into(vec![1, 2, 3]), vec![Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef])]
    #[case(dataframe!("data" => [true, true, false])?, Into::<PrimitiveArray<Int32Type>>::into(vec![1, 2, 3]), vec![Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef, Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef])]
    async fn test_add_any_num_col_to_df<T: ArrowPrimitiveType>(
        #[case] df: DataFrame,
        #[case] data: PrimitiveArray<T>,
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let res = add_any_num_col_to_df(&ctx, df, data, "new_col").await?;
        let batch = concat_df_batches(res).await?;
        let arrays: Vec<ArrayRef> = batch.columns().to_vec();
        assert_eq!(arrays, expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3])?, Into::<GenericByteArray<Utf8Type>>::into(vec!["foo", "bar", "baz"]), vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    #[case(dataframe!("name" => ["foo", "bar", "baz"])?, Into::<GenericByteArray<Utf8Type>>::into(vec!["foo", "bar", "baz"]), vec![Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    #[case(dataframe!("data" => [true, true, false])?, Into::<GenericByteArray<Utf8Type>>::into(vec!["foo", "bar", "baz"]), vec![Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    async fn test_add_any_str_col_to_df<T: ByteArrayType>(
        #[case] df: DataFrame,
        #[case] data: GenericByteArray<T>,
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let res = add_any_str_col_to_df(&ctx, df, data, "new_col").await?;
        let batch = concat_df_batches(res).await?;
        let arrays: Vec<ArrayRef> = batch.columns().to_vec();
        assert_eq!(arrays, expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3])?, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])), vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    #[case(dataframe!("name" => ["foo", "bar", "baz"])?, Arc::new(Int32Array::from(vec![1, 2, 3])), vec![Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef])]
    #[case(dataframe!("data" => [true, true, false])?, Arc::new(BooleanArray::from(vec![true, true, false])), vec![Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef, Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef])]
    async fn test_add_col_arr_to_df(
        #[case] df: DataFrame,
        #[case] data: ArrayRef,
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let res = add_col_arr_to_df(&ctx, df, &data, "new_col").await?;
        let batch = concat_df_batches(res).await?;
        let arrays: Vec<ArrayRef> = batch.columns().to_vec();
        assert_eq!(arrays, expected);
        Ok(())
    }
}