use std::io::Cursor;
use std::sync::Arc;

use color_eyre::eyre::Report;
use datafusion::arrow::array::{ArrayRef, StringArray, StructArray};
use datafusion::arrow::compute::{concat, concat_batches};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, ViewTable};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use futures_util::TryStreamExt;
use futures_util::future::try_join_all;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use serde_json::{Map, Value};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamExt;

use crate::error::UtilsError;

/// Query dataframe with sql
/// # Examples
/// ```
/// # use datafusion_example::utils::dataframe::df_sql;
/// use datafusion::prelude::*;
/// let df = dataframe!(
///     "id" => [1, 2, 3],
///     "name" => ["foo", "bar", "baz"]
/// ).unwrap();
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
    let mut stream = df.execute_stream().await?;
    if let Some(batch) = stream.next().await.transpose()? {
        if batch.num_rows() > 0 {
            return Ok(false);
        }
    }
    Ok(true)
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

/// Concatenates arrays per column for dataframe
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

/// Concatenates batches together into a single RecordBatch
pub async fn concat_df_batches(df: DataFrame) -> Result<RecordBatch, UtilsError> {
    let schema = df.schema().as_arrow().clone();
    let batches = df.collect().await?;
    let batch = concat_batches(&Arc::new(schema), &batches)?;
    Ok(batch)
}

/// Concat dataframes with the same schema into one dataframe
pub async fn concat_dfs(
    ctx: &SessionContext,
    dfs: Vec<DataFrame>,
) -> Result<DataFrame, UtilsError> {
    if dfs.is_empty() {
        return Err(UtilsError::UnexpectedError(Report::msg(
            "No dataframes provided",
        )));
    }
    let batches = collect_batches(dfs).await?;
    let res = ctx.read_batches(batches)?;
    Ok(res)
}

async fn collect_batches(dfs: Vec<DataFrame>) -> Result<Vec<RecordBatch>, DataFusionError> {
    try_join_all(dfs.into_iter().map(|df| async move { df.collect().await }))
        .await
        .map(|vec_of_batches| vec_of_batches.into_iter().flatten().collect())
}

/// Create json like string column new_col from cols
/// # Examples
/// ```
/// # use color_eyre::Result;
/// use datafusion::prelude::*;
/// # use datafusion_example::utils::dataframe::df_cols_to_json;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = dataframe!(
///     "id" => [1, 2, 3],
///     "name" => [Some("foo"), Some("bar"), None],
///     "data" => [42, 43, 44]
/// )?;
/// // +----+------+------+
/// // | id | name | data |
/// // +----+------+------+
/// // | 1  | foo  | 42   |
/// // | 2  | bar  | 43   |
/// // | 3  |      | 44   |
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
/// # Examples
/// ```
/// # use color_eyre::Result;
/// use datafusion::prelude::*;
/// # use datafusion_example::utils::dataframe::df_cols_to_struct;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = dataframe!(
///     "id" => [1, 2, 3],
///     "name" => ["foo", "bar", "baz"],
///     "data" => [42, 43, 44]
/// )?;
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
/// // Can de done using 'struct' in query:
/// // let res = ctx.sql("select id, struct(name as name, data as data) as new_col from t").await?;
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

fn make_new_df(
    ctx: &SessionContext,
    arrays: Vec<ArrayRef>,
    old_schema: &SchemaRef,
    col_name: &str,
    new_col_type: &DataType,
) -> Result<DataFrame, UtilsError> {
    let mut new_fields: Vec<Field> = old_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    new_fields.push(Field::new(col_name, new_col_type.clone(), true));
    let new_schema = Arc::new(Schema::new(new_fields));
    let batch = RecordBatch::try_new(new_schema, arrays)?;
    let df = ctx.read_batch(batch)?;
    Ok(df)
}

/// Add column to existing dataframe
/// # Examples
/// ```
/// use std::sync::Arc;
/// # use color_eyre::Result;
/// use datafusion::prelude::*;
/// use datafusion::arrow::array::{ArrayRef, StringArray};
/// # use datafusion_example::utils::dataframe::add_column_to_df;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = dataframe!("id" => [1, 2, 3])?;
/// let name: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));
/// let ctx = SessionContext::new();
/// let res = add_column_to_df(&ctx, df, name, "name").await?;
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
pub async fn add_column_to_df(
    ctx: &SessionContext,
    df: DataFrame,
    data: ArrayRef,
    col_name: &str,
) -> Result<DataFrame, UtilsError> {
    let schema = df.schema().as_arrow().clone();
    let mut arrays = concat_arrays(df).await?;
    let row_count = arrays
        .first()
        .ok_or_else(|| DataFusionError::Execution("Empty DataFrame".into()))?
        .len();

    if data.len() != row_count {
        return Err(DataFusionError::Execution(format!(
            "Column '{col_name}' has length {}, expected {row_count}",
            data.len()
        ))
        .into());
    }

    let new_col_type = data.data_type().clone();
    arrays.push(data);

    make_new_df(ctx, arrays, &Arc::new(schema), col_name, &new_col_type)
}

/// Add columns to existing dataframe
/// # Examples
/// ```
/// use std::sync::Arc;
/// # use color_eyre::Result;
/// use datafusion::prelude::*;
/// use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
/// # use datafusion_example::utils::dataframe::add_columns_to_df;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = dataframe!("id" => [1, 2, 3])?;
/// let name: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));
/// let data: ArrayRef = Arc::new(Int32Array::from(vec![42, 43, 44]));
/// let ctx = SessionContext::new();
/// let res = add_columns_to_df(&ctx, df, &[("name", name), ("data", data)]).await?;
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
pub async fn add_columns_to_df(
    ctx: &SessionContext,
    df: DataFrame,
    data: &[(&str, ArrayRef)],
) -> Result<DataFrame, UtilsError> {
    let schema = df.schema().as_arrow().clone();
    let mut arrays = concat_arrays(df).await?;
    let row_count = arrays
        .first()
        .ok_or_else(|| DataFusionError::Execution("Empty DataFrame".into()))?
        .len();

    let mut new_fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();

    for (col_name, col_data) in data {
        if col_data.len() != row_count {
            return Err(DataFusionError::Execution(format!(
                "Column '{col_name}' has length {}, expected {row_count}",
                col_data.len()
            ))
            .into());
        }

        arrays.push(col_data.clone());
        new_fields.push(Field::new(*col_name, col_data.data_type().clone(), true));
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    let final_batch = RecordBatch::try_new(new_schema, arrays)?;
    let res = ctx.read_batch(final_batch)?;
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
    let schema = df.schema().as_arrow().clone();
    let batches = df.collect().await?;
    let mem_table = MemTable::try_new(Arc::new(schema), vec![batches])?;
    ctx.register_table(table_name, Arc::new(mem_table))?;
    Ok(())
}

/// Register dataframe plan as table to query later
pub fn df_plan_to_table(
    ctx: &SessionContext,
    plan: LogicalPlan,
    table_name: &str,
) -> Result<(), UtilsError> {
    let view = ViewTable::new(plan, None);
    ctx.register_table(table_name, Arc::new(view))?;
    Ok(())
}

/// Convert dataframe to json like data that can be used later to store as string,
/// note that not all types can be serealized
/// # Examples
/// ```
/// # use color_eyre::Result;
/// use datafusion::prelude::*;
/// use serde_json::Value;
/// # use datafusion_example::utils::dataframe::df_to_json_bytes;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let df = dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?;
/// let res = df_to_json_bytes(df).await?;
/// let value: Value = serde_json::from_slice(&res)?;
/// assert_eq!(value.to_string(), r#"[{"id":1,"name":"foo"},{"id":2,"name":"bar"},{"id":3,"name":"baz"}]"#);
/// # Ok(())
/// # }
/// ```
pub async fn df_to_json_bytes(df: DataFrame) -> Result<Vec<u8>, UtilsError> {
    let batches = df.collect().await?;
    let buf = vec![];
    let mut writer = arrow_json::ArrayWriter::new(buf);
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    let data = writer.into_inner();
    Ok(data)
}

/// Join dataframes using same column name,
/// right columns used by join will be removed
/// # Examples
/// ```
/// # use color_eyre::Result;
/// use datafusion::prelude::*;
/// use serde_json::Value;
/// # use datafusion_example::utils::dataframe::join_dfs;
/// # fn main() -> Result<()> {
/// let df1 = dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?;
/// let df2 = dataframe!("id" => [1, 2, 3],"data" => [42, 43, 44])?;
/// let res = join_dfs(vec![df1, df2], &["id"])?;
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
pub fn join_dfs(dfs: Vec<DataFrame>, columns: &[&str]) -> Result<DataFrame, UtilsError> {
    let mut iter = dfs.into_iter();

    let mut res = match iter.next() {
        Some(df) => df,
        None => return Err(UtilsError::UnexpectedError(Report::msg("Empty dataframe"))),
    };

    for (i, mut df) in iter.enumerate() {
        let rhs_columns: Vec<String> = columns.iter().map(|c| format!("{c}_rhs{i}")).collect();

        for (old, new) in columns.iter().zip(&rhs_columns) {
            df = df.with_column_renamed(*old, new)?;
        }

        let rhs_refs: Vec<&str> = rhs_columns.iter().map(|c| c.as_str()).collect();
        res = res
            .join(df, JoinType::Inner, columns, &rhs_refs, None)?
            .drop_columns(&rhs_refs)?;
    }

    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::helpers::extract_struct_array_values;

    use color_eyre::Result;
    use datafusion::arrow::array::*;
    use datafusion::arrow::compute::concat_batches;
    use rstest::rstest;

    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, Some(vec!["id", "name", "data"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, Some(vec!["id", "name"]))]
    #[case(dataframe!("id" => [1, 2, 3],"data" => [42, 43, 44])?, Some(vec!["id", "data"]))]
    #[case(dataframe!("id" => [1, 2, 3])?, Some(vec!["id"]))]
    #[case(dataframe!()?, None)]
    fn test_get_column_names(
        #[case] df: DataFrame,
        #[case] expected: Option<Vec<&str>>,
    ) -> Result<()> {
        assert_eq!(expected, get_column_names(&df));
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, false)]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, false)]
    #[case(dataframe!("id" => [1, 2, 3],"data" => [42, 43, 44])?, false)]
    #[case(dataframe!("id" => [1, 2, 3])?, false)]
    #[case(dataframe!("id" => [1])?, false)]
    #[case(dataframe!("id" => [None::<i32>])?, false)]
    #[case(dataframe!()?, true)]
    async fn test_is_empty(#[case] df: DataFrame, #[case] expected: bool) -> Result<()> {
        assert_eq!(is_empty(df).await?, expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(vec![dataframe!("id" => [1, 2])?, dataframe!("id" => [3, 4])?], vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef])]
    #[case(vec![dataframe!("id" => [1, 2])?, dataframe!("id" => [3])?], vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef])]
    #[case(vec![dataframe!("id" => [1, 2, 3])?, dataframe!("id" => [4])?], vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef])]
    #[case(vec![dataframe!("name" => ["foo", "bar"])?, dataframe!("name" => ["baz"])?], vec![Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    async fn test_concat_dfs(
        #[case] dfs: Vec<DataFrame>,
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let df = concat_dfs(&ctx, dfs).await?;
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;
        let batch = concat_batches(&Arc::new(schema), &batches)?;
        let arrays: Vec<ArrayRef> = batch.columns().to_vec();
        assert_eq!(arrays, expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, Arc::new(Int32Array::from(vec![1, 2, 3])), Some(vec!["id", "name", "data", "new_col"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, Arc::new(Int32Array::from(vec![1, 2, 3])), Some(vec!["id", "name", "new_col"]))]
    #[case(dataframe!("id" => [1, 2, 3],"data" => [42, 43, 44])?, Arc::new(Int32Array::from(vec![1, 2, 3])), Some(vec!["id", "data", "new_col"]))]
    #[case(dataframe!("id" => [1, 2, 3])?, Arc::new(Int32Array::from(vec![1, 2, 3])), Some(vec!["id", "new_col"]))]
    async fn test_add_column_to_df_columns(
        #[case] df: DataFrame,
        #[case] data: ArrayRef,
        #[case] expected: Option<Vec<&str>>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let df = add_column_to_df(&ctx, df, data, "new_col").await?;
        let columns = get_column_names(&df);
        assert_eq!(columns, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_add_column_to_empty_df_columns() -> Result<()> {
        let ctx = SessionContext::new();
        let df = dataframe!()?; // empty df
        let col = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let result = add_column_to_df(&ctx, df, col, "new_col").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        dbg!(err_msg.clone());
        assert!(err_msg.contains("DataFusionError"), "Empty DataFrame");
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "name", "data"], Some(vec!["new_col"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "name"], Some(vec!["data", "new_col"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "data"], Some(vec!["name", "new_col"]))]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["name", "data"], Some(vec!["id", "new_col"]))]
    async fn test_cols_to_json_columns(
        #[case] df: DataFrame,
        #[case] cols: &[&str],
        #[case] expected: Option<Vec<&str>>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let df = df_cols_to_json(&ctx, df, cols, "new_col").await?;
        let columns = get_column_names(&df);
        assert_eq!(columns, expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, Arc::new(Int32Array::from(vec![1, 2, 3])), vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef])]
    #[case(dataframe!("id" => [1, 2, 3])?, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])), vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    #[case(dataframe!("id" => [1, 2, 3])?, Arc::new(Float32Array::from(vec![42., 43., 44.])), vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(Float32Array::from(vec![42., 43., 44.])) as ArrayRef])]
    #[case(dataframe!("id" => [1, 2, 3])?, Arc::new(BooleanArray::from(vec![true, true, false])), vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef])]
    async fn test_add_column_to_df(
        #[case] df: DataFrame,
        #[case] data: ArrayRef,
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let df = add_column_to_df(&ctx, df, data, "new_col").await?;
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;
        let batch = concat_batches(&Arc::new(schema), &batches)?;
        let arrays: Vec<ArrayRef> = batch.columns().to_vec();
        assert_eq!(arrays, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_add_column_to_df_err() -> Result<()> {
        let ctx = SessionContext::new();
        let df = dataframe![
            "id" => [1, 2, 3],
            "name" => ["foo", "bar", "baz"]
        ]?;
        let data: ArrayRef = Arc::new(StringArray::from(vec!["foo", "foo", "foo", "foo"]));
        let result = add_column_to_df(&ctx, df, data, "new_col").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("DataFusionError"),
            "Column new_col has length 4, expected 3"
        );
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, &[("data", Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef)], vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef])]
    #[case(dataframe!("id" => [1, 2, 3])?, &[("name", Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef), ("data", Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef)], vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef])]
    #[case(dataframe!("id" => [1, 2, 3])?, &[("data", Arc::new(Float32Array::from(vec![42., 43., 44.])) as ArrayRef), ("name", Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef)], vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(Float32Array::from(vec![42., 43., 44.])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    #[case(dataframe!("id" => [1, 2, 3])?, &[("data", Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef), ("name", Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef)], vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    async fn test_add_columns_to_df(
        #[case] df: DataFrame,
        #[case] data: &[(&str, ArrayRef)],
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let df = add_columns_to_df(&ctx, df, data).await?;
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;
        let batch = concat_batches(&Arc::new(schema), &batches)?;
        let arrays: Vec<ArrayRef> = batch.columns().to_vec();
        assert_eq!(arrays, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_cols_to_json_col_is_not_found_columns() -> Result<()> {
        let ctx = SessionContext::new();
        let df = dataframe!(
            "id" => [1, 2, 3],
            "name" => ["foo", "bar", "baz"]
        )?;
        let result = df_cols_to_json(&ctx, df, &["column-doesnnot-exist"], "new_col").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("DataFusionError"), "column foo not found");
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "name", "data"], vec![Some(r#"{"data":42,"id":1,"name":"foo"}"#), Some(r#"{"data":43,"id":2,"name":"bar"}"#), Some(r#"{"data":44,"id":3,"name":"baz"}"#)])]
    #[case(dataframe!("id" => [Some(1), Some(2), None],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "name", "data"], vec![Some(r#"{"data":42,"id":1,"name":"foo"}"#), Some(r#"{"data":43,"id":2,"name":"bar"}"#), Some(r#"{"data":44,"name":"baz"}"#)])]
    #[case(dataframe!("id" => [None::<i32>, None, None],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "name", "data"], vec![Some(r#"{"data":42,"name":"foo"}"#), Some(r#"{"data":43,"name":"bar"}"#), Some(r#"{"data":44,"name":"baz"}"#)])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "data"], vec![Some(r#"{"data":42,"id":1}"#), Some(r#"{"data":43,"id":2}"#), Some(r#"{"data":44,"id":3}"#)])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "name"], vec![Some(r#"{"id":1,"name":"foo"}"#), Some(r#"{"id":2,"name":"bar"}"#), Some(r#"{"id":3,"name":"baz"}"#)])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["name", "data"], vec![Some(r#"{"data":42,"name":"foo"}"#), Some(r#"{"data":43,"name":"bar"}"#), Some(r#"{"data":44,"name":"baz"}"#)])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["name"], vec![Some(r#"{"name":"foo"}"#), Some(r#"{"name":"bar"}"#), Some(r#"{"name":"baz"}"#)])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["data"], vec![Some(r#"{"data":42}"#), Some(r#"{"data":43}"#), Some(r#"{"data":44}"#)])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id"], vec![Some(r#"{"id":1}"#), Some(r#"{"id":2}"#), Some(r#"{"id":3}"#)])]
    async fn test_cols_to_json(
        #[case] df: DataFrame,
        #[case] cols: &[&str],
        #[case] expected: Vec<Option<&str>>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let df = df_cols_to_json(&ctx, df, cols, "new_col").await?;
        let batches = df.select_columns(&["new_col"])?.collect().await?;
        let mut values = Vec::new();
        for batch in batches.iter() {
            let col_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");

            for i in 0..col_array.len() {
                if col_array.is_valid(i) {
                    values.push(Some(col_array.value(i)));
                } else {
                    values.push(None);
                }
            }
        }
        assert_eq!(values, expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, &["id", "name", "data"], vec![vec!["1", "foo", "42"], vec!["2", "bar", "43"], vec!["3", "baz", "44"]])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [true, true, false])?, &["id", "name", "data"], vec![vec!["1", "foo", "true"], vec!["2", "bar", "true"], vec!["3", "baz", "false"]])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, &["id", "name"], vec![vec!["1", "foo"], vec!["2", "bar"], vec!["3", "baz"]])]
    #[case(dataframe!("id" => [1, 2, 3])?, &["id"], vec![vec!["1"], vec!["2"], vec!["3"]])]
    async fn test_cols_to_struct(
        #[case] df: DataFrame,
        #[case] cols: &[&str],
        #[case] expected: Vec<Vec<&str>>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        let df = df_cols_to_struct(&ctx, df, cols, "new_col")
            .await?
            .select_columns(&["new_col"])?;
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;
        let batch = concat_batches(&Arc::new(schema), &batches)?;
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let res = extract_struct_array_values(array);
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_cols_to_struct_err() -> Result<()> {
        let ctx = SessionContext::new();
        let df = dataframe!(
            "id" => [1, 2, 3],
            "name" => ["foo", "bar", "baz"]
        )?;
        let result = df_cols_to_struct(&ctx, df, &["foo"], "new_col").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("DataFusionError"), "column foo not found");
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, vec![3, 3])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, vec![2, 3])]
    #[case(dataframe!("id" => [1, 2, 3])?, vec![1, 3])]
    #[case(dataframe!("id" => [1, 2])?, vec![1, 2])]
    #[case(dataframe!()?, vec![0, 0])]
    async fn test_concat_df_batches_cols_rows(
        #[case] df: DataFrame,
        #[case] expected: Vec<usize>,
    ) -> Result<()> {
        let batch = concat_df_batches(df).await?;
        let res = vec![batch.num_columns(), batch.num_rows()];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    #[case(dataframe!("id" => [1, 2, 3])?, vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef])]
    async fn test_concat_df_batches(
        #[case] df: DataFrame,
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let batch = concat_df_batches(df).await?;
        let arrays: Vec<ArrayRef> = batch.columns().to_vec();
        assert_eq!(arrays, expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, vec![3, 3])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, vec![2, 3])]
    #[case(dataframe!("id" => [1, 2, 3])?, vec![1, 3])]
    #[case(dataframe!("id" => [1, 2])?, vec![1, 2])]
    async fn test_concat_arrays_cols_rows(
        #[case] df: DataFrame,
        #[case] expected: Vec<usize>,
    ) -> Result<()> {
        let schema = df.schema().as_arrow().clone();
        let arrays: Vec<ArrayRef> = concat_arrays(df).await?;
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;
        let res = vec![batch.num_columns(), batch.num_rows()];
        assert_eq!(res, expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef])]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef])]
    #[case(dataframe!("id" => [1, 2, 3])?, vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef])]
    async fn test_concat_arrays(
        #[case] df: DataFrame,
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let arrays: Vec<ArrayRef> = concat_arrays(df).await?;
        assert_eq!(arrays, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_concat_arrays_err() -> Result<()> {
        let df = dataframe!()?;
        let schema = df.schema().as_arrow().clone();
        let arrays: Vec<ArrayRef> = concat_arrays(df).await?;
        let result = RecordBatch::try_new(Arc::new(schema), arrays);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains(
            "Invalid argument error: must either specify a row count or at least one column"
        ));
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"data" => [42, 43, 44])?, r#"[{"data":42,"id":1,"name":"foo"},{"data":43,"id":2,"name":"bar"},{"data":44,"id":3,"name":"baz"}]"#)]
    #[case(dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, r#"[{"id":1,"name":"foo"},{"id":2,"name":"bar"},{"id":3,"name":"baz"}]"#)]
    #[case(dataframe!("id" => [1, 2, 3])?, r#"[{"id":1},{"id":2},{"id":3}]"#)]
    async fn test_df_to_json_bytes(#[case] df: DataFrame, #[case] expected: &str) -> Result<()> {
        let res = df_to_json_bytes(df).await?;
        let value: Value = serde_json::from_slice(&res)?;
        assert_eq!(value.to_string(), expected);
        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(vec![dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, dataframe!("id" => [1, 2, 3],"data" => [42, 43, 44])?], &["id"], vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef])]
    #[case(vec![dataframe!("id" => [1, 2, 3],"name" => ["foo", "bar", "baz"],"value" => ["foo", "bar", "baz"])?, dataframe!("id" => [1, 2, 3],"data" => [42, 43, 44])?], &["id"], vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef])]
    #[case(vec![dataframe!("id" => [1, 2, 3],"pk" => [1, 2, 3],"name" => ["foo", "bar", "baz"])?, dataframe!("id" => [1, 2, 3],"pk" => [1, 2, 3],"data" => [42, 43, 44])?], &["id", "pk"], vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef, Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef])]
    async fn test_join_dfs(
        #[case] dfs: Vec<DataFrame>,
        #[case] columns: &[&str],
        #[case] expected: Vec<ArrayRef>,
    ) -> Result<()> {
        let res = join_dfs(dfs, columns)?;
        let batch = concat_df_batches(res).await?;
        let arrays: Vec<ArrayRef> = batch.columns().to_vec();
        assert_eq!(arrays, expected);
        Ok(())
    }
}
