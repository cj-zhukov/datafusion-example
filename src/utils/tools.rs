use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, GenericByteArray,
    Int32Array, Int64Array, PrimitiveArray, StringArray, StructArray,
};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, ByteArrayType, DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, ViewTable};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use futures_util::TryStreamExt;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use serde_json::{Map, Value};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamExt;

use crate::error::UtilsError;

/// Macro for creating dataframe, similar (almost) to polars
/// # Examples
/// ```
/// use datafusion::arrow::array::{Int32Array, StringArray};
/// # use datafusion_example::df;
/// let id = Int32Array::from(vec![1, 2, 3]);
/// let name = StringArray::from(vec!["foo", "bar", "baz"]);
/// let df = df!(
///    "id" => id,
///    "name" => name
///  );
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// ```
#[macro_export]
macro_rules! df {
    () => {{
        use datafusion::prelude::*;

        let ctx = SessionContext::new();
        ctx.read_empty().expect("failed creating empty dataframe")
    }};

    // ($($col_name:expr_2021 => $data:expr_2021),+) => {{ // #TODO expr_2021 vs expr
    ($($col_name:expr => $data:expr),+) => {{
        use datafusion::prelude::*;
        use datafusion::arrow::array::{RecordBatch, ArrayRef};
        use datafusion::arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        let mut fields = vec![];
        let mut columns: Vec<ArrayRef> = vec![];

        $(
            let col = Arc::new($data) as ArrayRef;
            let dtype = col.data_type().clone();
            fields.push(Field::new($col_name, dtype, true));
            columns.push(col);
        )+

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns).expect("failed creating batch");
        let ctx = SessionContext::new();
        ctx.read_batch(batch).expect("failed creating dataframe")
    }};
}

/// Query dataframe with sql
/// # Examples
/// ```
/// use datafusion::arrow::array::{Int32Array, StringArray};
/// # use datafusion_example::{df, utils::tools::df_sql};
/// let id = Int32Array::from(vec![1, 2, 3]);
/// let name = StringArray::from(vec!["foo", "bar", "baz"]);
/// let df = df!("id" => id, "name" => name);
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
/// use datafusion::arrow::array::{Int32Array, StringArray};
/// # use datafusion_example::{df, utils::tools::add_pk_to_df};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let id = Int32Array::from(vec![1, 2, 3]);
/// let name = StringArray::from(vec!["foo", "bar", "baz"]);
/// let df = df!("id" => id, "name" => name);
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
    let max_len = arrays.first().unwrap().len();
    let pks: ArrayRef = Arc::new(Int32Array::from_iter(0..max_len as i32));
    arrays.push(pks);
    let schema_pks = Schema::new(vec![Field::new(col_name, DataType::Int32, true)]);
    let schema_new = Schema::try_merge(vec![schema, schema_pks])?;
    let batch = RecordBatch::try_new(Arc::new(schema_new), arrays)?;
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
/// use datafusion::arrow::array::{Int32Array, StringArray};
/// # use datafusion_example::{df, utils::tools::add_col_to_df};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let id = Int32Array::from(vec![1, 2, 3]);
/// let df = df!("id" => id);
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
    let schema = df.schema().as_arrow().clone();
    let mut arrays = concat_arrays(df).await?;
    let schema_new_col = Schema::new(vec![Field::new(col_name, data.data_type().clone(), true)]);
    arrays.push(data);
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
/// use datafusion::arrow::array::{Int32Array, StringArray};
/// # use datafusion_example::{df, utils::tools::add_col_arr_to_df};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let id = Int32Array::from(vec![1, 2, 3]);
/// let df = df!("id" => id);
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
/// use datafusion::arrow::array::{Int32Array, StringArray};
/// # use datafusion_example::{df, utils::tools::select_all_exclude};
/// let id = Int32Array::from(vec![1, 2, 3]);
/// let name = StringArray::from(vec!["foo", "bar", "baz"]);
/// let data = Int32Array::from(vec![42, 43, 44]);
/// let df = df!("id" => id, "name" => name, "data" => data);
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
    let columns = df
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .filter(|x| !to_exclude.iter().any(|col| col.eq(x)))
        .collect::<Vec<_>>();
    let res = df.clone().select_columns(&columns)?;
    Ok(res)
}

/// Get dataframe columns names
pub fn get_column_names(df: &DataFrame) -> Vec<&str> {
    df.schema()
        .fields()
        .iter()
        .map(|col| col.name().as_str())
        .collect::<Vec<_>>()
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
/// use datafusion::arrow::array::{Int32Array, StringArray};
/// # use datafusion_example::{df, utils::tools::df_cols_to_json};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let id = Int32Array::from(vec![1, 2, 3]);
/// let name = StringArray::from(vec!["foo", "bar", "baz"]);
/// let data = Int32Array::from(vec![42, 43, 44]);
/// let df = df!("id" => id, "name" => name, "data" => data);
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
/// // | 3  | {"data":44,"name":"baz"} |,
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
    let schema = df.schema().as_arrow().to_owned();
    let schema_ref = Arc::new(schema);
    let mut arrays = concat_arrays(df).await?;
    let batch = RecordBatch::try_new(schema_ref.clone(), arrays.clone())?;
    let df_prepared = ctx.read_batch(batch)?;
    let batches = df_prepared.select_columns(cols)?.collect().await?;
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;

    let mut str_rows = vec![];
    for json_row in &json_rows {
        let str_row = if !json_rows.is_empty() {
            let str_row = serde_json::to_string(&json_row)?;
            Some(str_row)
        } else {
            None
        };
        str_rows.push(str_row);
    }

    let new_col_arr: ArrayRef = Arc::new(StringArray::from(str_rows));
    arrays.push(new_col_arr);

    let schema_new_col = Schema::new(vec![Field::new(new_col, DataType::Utf8, true)]);
    let schema = schema_ref.as_ref().clone();
    let schema_new = Schema::try_merge(vec![schema, schema_new_col])?;
    let batch = RecordBatch::try_new(Arc::new(schema_new), arrays)?;
    let res = ctx.read_batch(batch)?;
    let res = res.drop_columns(cols)?;
    Ok(res)
}

/// Create nested struct column new_col from cols.
/// Can de done using 'struct' in query: ctx.sql("select id, struct(name as name, data as data) as new_col from t").await?;
/// # Examples
/// ```
/// # use color_eyre::Result;
/// use datafusion::prelude::*;
/// use datafusion::arrow::array::{Int32Array, StringArray};
/// # use datafusion_example::{df, utils::tools::df_cols_to_struct};
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let id = Int32Array::from(vec![1, 2, 3]);
/// let name = StringArray::from(vec!["foo", "bar", "baz"]);
/// let data = Int32Array::from(vec![42, 43, 44]);
/// let df = df!("id" => id, "name" => name, "data" => data);
/// // +----+------+------+
/// // | id | name | data |
/// // +----+------+------+
/// // | 1  | foo  | 42   |
/// // | 2  | bar  | 43   |
/// // | 3  | baz  | 44   |
/// // +----+------+------+
/// let ctx = SessionContext::new();
/// //
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
    let schema = df.schema().as_arrow().to_owned();
    let schema_ref = Arc::new(schema);
    let mut arrays = concat_arrays(df).await?;
    let batch = RecordBatch::try_new(schema_ref.clone(), arrays.clone())?;
    let mut struct_array_data = vec![];
    for col in cols {
        let field = schema_ref.field_with_name(col)?.clone();
        let arr = batch.column_by_name(col).unwrap().clone();
        struct_array_data.push((Arc::new(field), arr));
    }
    let df_struct = ctx.read_batch(batch)?.select_columns(cols)?;
    let fields = df_struct.schema().clone().as_arrow().fields().clone();
    let struct_array = StructArray::from(struct_array_data);
    let struct_array_schema =
        Schema::new(vec![Field::new(new_col, DataType::Struct(fields), true)]);
    let schema = schema_ref.as_ref().clone();
    let schema_new = Schema::try_merge(vec![schema, struct_array_schema])?;
    arrays.push(Arc::new(struct_array));
    let batch_with_struct = RecordBatch::try_new(Arc::new(schema_new), arrays)?;

    let res = ctx.read_batch(batch_with_struct)?;
    let res = res.drop_columns(cols)?;
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
    let view = ViewTable::try_new(plan, None)?;
    ctx.register_table(table_name, Arc::new(view))?;
    Ok(())
}