use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, GenericByteArray,
    Int32Array, Int64Array, PrimitiveArray, StringArray, StructArray,
};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, ByteArrayType, DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, ViewTable};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use futures_util::TryStreamExt;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use serde_json::{Map, Value};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamExt;

use crate::error::UtilsError;

/// Query dataframe with sql
/// # Examples
/// ```
/// # use datafusion_example::{df, utils::tools::df_sql};
/// let df = df!(
///     "id" => vec![1, 2, 3], 
///     "name" => vec!["foo", "bar", "baz"]
/// );
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// let sql = r#"id > 2 and name in ('foo', 'bar', 'baz')"#;
/// let res = df_sql(df, sql);
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 3  | baz  |,
/// // +----+------+,
/// ```
pub async fn df_sql(df: DataFrame, sql: &str) -> Result<DataFrame, UtilsError> {
    let filter = df.parse_sql_expr(sql)?;
    let res = df.filter(filter)?;
    Ok(res)
}

/// Check if dataframe is empty and doesn't have rows
pub async fn is_empty(df: DataFrame) -> Result<bool, UtilsError> {
    let batches = df.collect().await?;
    let is_empty = batches.iter().all(|batch| batch.num_rows() == 0);
    Ok(is_empty)
}

/// Add auto-increment column to dataframe
/// # Examples
/// ```
/// use datafusion::prelude::*;
/// # use color_eyre::Result;
/// # use datafusion_example::{df, utils::tools::add_pk_to_df};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = df!(
///     "id" => vec![1, 2, 3], 
///     "name" => vec!["foo", "bar", "baz"]
/// );
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

    let mut new_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())   
        .collect();
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
/// # use datafusion_example::{df, utils::tools::add_col_to_df};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = df!("id" => vec![1, 2, 3]);
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
    let row_count = arrays.first()
        .ok_or_else(|| DataFusionError::Execution("empty DataFrame".into()))?
        .len();

    if data.len() != row_count {
        return Err(DataFusionError::Execution(
            format!(
                "new column '{col_name}' has length {}, expected {row_count}",
                data.len()
            ),
        ).into());
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
/// # use datafusion_example::{df, utils::tools::add_col_arr_to_df};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = df!("id" => vec![1, 2, 3]);
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
/// # use datafusion_example::{df, utils::tools::select_all_exclude};
/// let df = df!(
///     "id" => vec![1, 2, 3], 
///     "name" => vec!["foo", "bar", "baz"], 
///     "data" => vec![42, 43, 44]
/// );
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

/// Returns column names if the schema is not empty.
pub fn get_column_names(df: &DataFrame) -> Option<Vec<&str>> {
    let fields = df.schema().fields();
    if fields.is_empty() {
        return None;
    }
    Some(
        fields
            .iter()
            .map(|col| col.name().as_str())
            .collect::<Vec<_>>(),
    )
}

/// Concat arrays per column for dataframe
pub async fn concat_arrays(df: DataFrame) -> Result<Vec<ArrayRef>, UtilsError> {
    let schema = df.schema().clone();
    let batches = df.collect().await?;
    let batches = batches.iter().collect::<Vec<_>>();
    let field_num = schema.fields().len();
    let mut arrays = Vec::with_capacity(field_num);
    for i in 0..field_num {
        let array = batches
            .iter()
            .map(|batch| batch.column(i).as_ref())
            .collect::<Vec<_>>();
        let array = concat(&array)?;

        arrays.push(array);
    }
    Ok(arrays)
}

/// Concat dataframes with the same schema into one dataframe
pub async fn concat_dfs(
    ctx: &SessionContext,
    dfs: Vec<DataFrame>,
) -> Result<DataFrame, UtilsError> {
    let mut batches = vec![];
    for df in dfs {
        let batch = df.collect().await?;
        batches.extend(batch);
    }
    let res = ctx.read_batches(batches)?;
    Ok(res)
}

/// Create json like string column new_col from cols
/// # Examples
/// ```
/// # use color_eyre::Result;
/// use datafusion::prelude::*;
/// # use datafusion_example::{df, utils::tools::df_cols_to_json};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = df!(
///     "id" => vec![1, 2, 3], 
///     "name" => vec![Some("foo"), Some("bar"), None], 
///     "data" => vec![42, 43, 44]
/// );
/// // +----+------+------+
/// // | id | name | data |
/// // +----+------+------+
/// // | 1  | foo  | 42   |
/// // | 2  | bar  | 43   |
/// // | 3  | baz  | 44   |
/// // +----+------+------+
/// let ctx = SessionContext::new();
/// let res = df_cols_to_json(&ctx, df, &["name", "data"], "new_col").await?;
/// // +----+--------------------------+,
/// // | id | new_col                  |,
/// // +----+--------------------------+,
/// // | 1  | {"data":42,"name":"foo"} |,
/// // | 2  | {"data":43,"name":"bar"} |,
/// // | 3  | {"data":44}              |,
/// // +----+--------------------------+,
/// # Ok(())
/// # }
/// ```
pub async fn df_cols_to_json(
    ctx: &SessionContext,
    df: DataFrame,
    cols: &[&str],
    new_col: &str,
) -> Result<DataFrame, UtilsError> {
    let schema_ref = Arc::new(df.schema().as_arrow().clone());
    let arrays = concat_arrays(df).await?;
    let batch = RecordBatch::try_new(schema_ref.clone(), arrays)?;

    let selected_arrays: Vec<ArrayRef> = cols
        .iter()
        .map(|col| {
            batch
                .column_by_name(col)
                .ok_or_else(|| DataFusionError::Plan(format!("column {col} not found")))
                .cloned()
        })
        .collect::<Result<_, _>>()?;

    let selected_fields: Vec<Field> = cols
        .iter()
        .map(|col| schema_ref.field_with_name(col).cloned())
        .collect::<Result<_, _>>()?;

    let selected_schema = Arc::new(Schema::new(selected_fields));
    let selected_batch = RecordBatch::try_new(selected_schema.clone(), selected_arrays)?;

    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    writer.write(&selected_batch)?;
    writer.finish()?;

    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;
    let str_rows: Vec<String> = json_rows
        .iter()
        .map(|row| serde_json::to_string(row))
        .collect::<Result<_, _>>()?;

    let mut all_columns = batch.columns().to_vec();
    all_columns.push(Arc::new(StringArray::from(str_rows)) as ArrayRef);

    let mut new_fields: Vec<Field> = schema_ref
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    new_fields.push(Field::new(new_col, DataType::Utf8, true));
    let new_schema = Arc::new(Schema::new(new_fields));

    let new_batch = RecordBatch::try_new(new_schema, all_columns)?;
    let res = ctx.read_batch(new_batch)?.drop_columns(cols)?;
    Ok(res)
}

/// Create nested struct column new_col from cols.
/// Can de done using 'struct' in query: ctx.sql("select id, struct(name as name, data as data) as new_col from t").await?;
/// # Examples
/// ```
/// # use color_eyre::Result;
/// use datafusion::prelude::*;
/// # use datafusion_example::{df, utils::tools::df_cols_to_struct};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = df!(
///     "id" => vec![1, 2, 3], 
///     "name" => vec!["foo", "bar", "baz"], 
///     "data" => vec![42, 43, 44]
/// );
/// // +----+------+------+
/// // | id | name | data |
/// // +----+------+------+
/// // | 1  | foo  | 42   |
/// // | 2  | bar  | 43   |
/// // | 3  | baz  | 44   |
/// // +----+------+------+
/// let ctx = SessionContext::new();
/// let res = df_cols_to_struct(&ctx, df, &["name", "data"], "new_col").await;
/// // +----+-----------------------+
/// // | id | new_col               |
/// // +----+-----------------------+
/// // | 1  | {name: foo, data: 42} |
/// // | 2  | {name: bar, data: 43} |
/// // | 3  | {name: baz, data: 44} |
/// // +----+-----------------------+
/// # Ok(())
/// # }
/// ```
pub async fn df_cols_to_struct(
    ctx: &SessionContext,
    df: DataFrame,
    cols: &[&str],
    new_col: &str,
) -> Result<DataFrame, UtilsError> {
    let schema_ref = Arc::new(df.schema().as_arrow().clone());
    let arrays = concat_arrays(df).await?;
    let batch = RecordBatch::try_new(schema_ref.clone(), arrays)?;

    let struct_array_data: Vec<_> = cols
        .iter()
        .map(|col| {
            let field = schema_ref.field_with_name(col)?.clone();
            let arr = batch
                .column_by_name(col)
                .ok_or_else(|| DataFusionError::Plan(format!("column {col} not found")))?
                .clone();
            Ok((Arc::new(field), arr))
        })
        .collect::<Result<_, DataFusionError>>()?;
    let struct_array = StructArray::from(struct_array_data);

    let mut new_columns = batch.columns().to_vec();
    new_columns.push(Arc::new(struct_array));

    let mut new_fields: Vec<Field> = schema_ref
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();

    let struct_fields: Vec<Field> = cols
        .iter()
        .map(|c| schema_ref.field_with_name(c).cloned())
        .collect::<Result<_, ArrowError>>()?;

    new_fields.push(Field::new(
        new_col,
        DataType::Struct(struct_fields.into()),
        true,
    ));
    let merged_schema = Arc::new(Schema::new(new_fields));

    let final_batch = RecordBatch::try_new(merged_schema, new_columns)?;
    let res = ctx.read_batch(final_batch)?.drop_columns(cols)?;
    Ok(res)
}

/// Read parquet file to dataframe
pub async fn read_file_to_df(
    ctx: &SessionContext,
    file_path: &str,
) -> Result<DataFrame, UtilsError> {
    let mut file = File::open(file_path).await?;
    let mut buffer = [0; 1024];
    let mut data = vec![];
    loop {
        let n = file.read(&mut buffer[..]).await?;
        if n == 0 {
            break;
        }
        data.extend_from_slice(&buffer[..n]);
    }
    let stream = ParquetRecordBatchStreamBuilder::new(Cursor::new(data))
        .await?
        .build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    let df = ctx.read_batches(batches)?;
    Ok(df)
}

/// Write dataframe to parquet file
pub async fn write_df_to_file(df: DataFrame, file_path: &str) -> Result<(), UtilsError> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, Arc::new(schema), None)?;
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch).await?;
    }
    writer.close().await?;
    let mut file = File::create(file_path).await?;
    file.write_all(&buf).await?;
    Ok(())
}

/// Register dataframe as table to query later
pub async fn df_to_table(
    ctx: &SessionContext,
    df: DataFrame,
    table_name: &str,
) -> Result<(), UtilsError> {
    let schema = df.clone().schema().as_arrow().clone();
    let batches = df.collect().await?;
    let mem_table = MemTable::try_new(Arc::new(schema), vec![batches])?;
    ctx.register_table(table_name, Arc::new(mem_table))?;
    Ok(())
}

/// Register dataframe plan as table to query later
pub async fn df_plan_to_table(
    ctx: &SessionContext,
    plan: LogicalPlan,
    table_name: &str,
) -> Result<(), UtilsError> {
    let view = ViewTable::new(plan, None);
    ctx.register_table(table_name, Arc::new(view))?;
    Ok(())
}
