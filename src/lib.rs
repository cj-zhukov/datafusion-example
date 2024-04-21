use std::collections::HashMap;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use awscreds::Credentials;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use datafusion::arrow::array::{builder, make_array, Array, ArrayData, ArrayDataBuilder, ArrayRef, AsArray, BooleanArray, Float64Array, Int32Array, Int32Builder, ListArray, ListBuilder, PrimitiveArray, StringArray, StructArray};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, GenericStringType, Int32Type, Schema, UInt32Type, Utf8Type};
use datafusion::arrow::record_batch::RecordBatch;
// use datafusion::config::ConfigOptions;
use datafusion::dataframe::DataFrameWriteOptions;
// use datafusion::logical_expr::expr::Unnest;
use datafusion::arrow;
use datafusion::scalar::ScalarValue;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use parquet::arrow::AsyncArrowWriter;
use serde_json::{Map, Value};
use itertools::{izip, Itertools};
use tokio_stream::StreamExt;
use url::Url;

fn parse_to_primitive<'a, T, I>(iter: I) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: FromStr,
    I: IntoIterator<Item=&'a str>,
{
    PrimitiveArray::from_iter(iter.into_iter().map(|val| T::Native::from_str(val).ok()))
}

fn parse_strings<'a, I>(iter: I, to_data_type: DataType) -> ArrayRef
where
    I: IntoIterator<Item=&'a str>,
{
   match to_data_type {
       DataType::Int32 => Arc::new(parse_to_primitive::<Int32Type, _>(iter)) as _,
       DataType::UInt32 => Arc::new(parse_to_primitive::<UInt32Type, _>(iter)) as _,
       _ => unimplemented!()
   }
}

pub fn downcast_example() {
    let array = parse_strings(["1", "2", "3"], DataType::Int32);
    let integers = array.as_any().downcast_ref::<Int32Array>().unwrap();
    let vals = integers.values();
    assert_eq!(vals, &[1, 2, 3]);

    let scalars = vec![
        ScalarValue::Int32(Some(1)),
        ScalarValue::Int32(None),
        ScalarValue::Int32(Some(2))
    ];
    let result = ScalarValue::new_list_from_iter(scalars.into_iter(), &DataType::Int32);
    let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![
        Some(vec![Some(1), None, Some(2)])
        ]);
    assert_eq!(*result, expected);
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValueNew {
    Boolean(Option<bool>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Utf8(Option<String>),
    LargeUtf8(Option<String>),
    List(Option<Vec<ScalarValue>>, DataType),
    Date32(Option<i32>),
    TimeMicrosecond(Option<i64>),
    TimeNanosecond(Option<i64>),
}

macro_rules! typed_cast {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        ScalarValueNew::$SCALAR(match array.is_null($index) {
            true => None,
            false => Some(array.value($index).into()),
        })
    }};
}

impl ScalarValueNew {
    pub fn try_from_array(array: &ArrayRef, index: usize) -> Result<Self, String> {
        Ok(match array.data_type() {
            DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean),
            DataType::Int32 => typed_cast!(array, index, Int32Array, Int32),
            DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8),
            other => {
                return Err(format!("Downcast not available for type: {}", other));
            }
        })
    }
}

pub async fn assert_example() -> anyhow::Result<()> {
    let ctx = SessionContext::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;
    df.clone().show().await?;
    let rows_count = df.clone().count().await?;
    let columns_count = df.clone().schema().field_names().len();
    assert_eq!(rows_count, 3);
    assert_eq!(columns_count, 2);

    let results: Vec<RecordBatch> = df.collect().await?;
    let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?.to_string();
    let expected = vec![
        "+----+------+",
        "| id | data |",
        "+----+------+",
        "| 1  | 42   |",
        "| 2  | 43   |",
        "| 3  | 44   |",
        "+----+------+",
    ];
    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}


pub async fn add_column_with_scalar_new_example() -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    let schema1 = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch1 = RecordBatch::try_new(
        schema1.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;
    let df1 = ctx.read_batch(batch1.clone())?;

    let schema2 = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch2 = RecordBatch::try_new(
        schema2.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df2 = ctx
        .read_batch(batch2.clone())?
        .with_column_renamed("id", "id2")?;

    let df = df1
        .join(df2, JoinType::Inner, &["id"], &["id2"], None)?
        .select_columns(&["id", "name", "data"])?;

    let mut stream = df.execute_stream().await?; 
    let mut columns: HashMap<usize, Vec<ScalarValueNew>> = HashMap::new();
    while let Some(batch) = stream.next().await.transpose()? {
        for i in 0..batch.num_columns() {
            let arr = batch.column(i);
            let value = ScalarValueNew::try_from_array(arr, 0).unwrap();
            let data = vec![value];
            match columns.get_mut(&i) {
                Some(val) => {
                    val.extend(data);
                },
                None => {
                    columns.insert(i, data);
                }
            }
        }        
    }
    println!("columns: {:?}", columns);
    println!("len: {:?}", columns.len());

    let mut id_all = vec![];
    let x = columns[&0].clone();
    for column in x {
        if let ScalarValueNew::Int32(res) = column {
            id_all.push(res);
        }
    }
    let mut name_all = vec![];
    let x = columns[&1].clone();
    for column in x {
        if let ScalarValueNew::Utf8(res) = column {
            name_all.push(res);
        }
    }
    let mut data_all = vec![];
    let x = columns[&2].clone();
    for column in x {
        if let ScalarValueNew::Int32(res) = column {
            data_all.push(res);
        }
    }

    let id: ArrayRef = Arc::new(Int32Array::from(id_all));
    let name: ArrayRef = Arc::new(StringArray::from(name_all));
    let data: ArrayRef = Arc::new(Int32Array::from(data_all));
    let record_batch = RecordBatch::try_from_iter_with_nullable(vec![
        ("id", id, true),
        ("name", name, true),
        ("data", data, true),
    ])?;
    let df = ctx.read_batch(record_batch)?;
    df.show().await?;

    Ok(())
}

pub async fn dev2() -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    let schema1 = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch1 = RecordBatch::try_new(
        schema1.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;
    let df1 = ctx.read_batch(batch1.clone())?;

    let schema2 = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch2 = RecordBatch::try_new(
        schema2.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df2 = ctx
        .read_batch(batch2.clone())?
        .with_column_renamed("id", "id2")?;

    let df = df1
        .join(df2, JoinType::Inner, &["id"], &["id2"], None)?
        .select_columns(&["id", "name", "data"])?;

    let mut stream = df.execute_stream().await?;
    let mut list = vec![];
    while let Some(b) = stream.next().await.transpose()? {
        let id_col = b.column(0).as_primitive::<Int32Type>();
        // let name_col = b.column(1).as_primitive::<GenericStringType<i32>>();
        let data_col = b.column(2).as_primitive::<Int32Type>();

        // for (id, name, data) in izip!(id_col.values(), name_col.values(), data_col.values()) {
        //     list.push((id, name, data));
        // }
        for (id, data) in izip!(id_col.values(), data_col.values()) {
            list.push((*id, *data));
        }
    }
    // println!("columns: {:?}", columns);
    // println!("len: {:?}", columns.len());

    println!("{:?}", list);

    // println!("{:?}", record_batch);
    // let df = ctx.read_batch(record_batch)?;
    let df = ctx.read_empty()?;
    df.show().await?;

    Ok(())
}

pub async fn dev() -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    let schema1 = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch1 = RecordBatch::try_new(
        schema1.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;
    let df1 = ctx.read_batch(batch1.clone())?;

    let schema2 = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch2 = RecordBatch::try_new(
        schema2.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df2 = ctx
        .read_batch(batch2.clone())?
        .with_column_renamed("id", "id2")?;

    let df = df1
        .join(df2, JoinType::Inner, &["id"], &["id2"], None)?
        .select_columns(&["id", "name", "data"])?;
    df.clone().show().await?;

    let df_cols_for_json = df.clone().select_columns(&["name", "data", "id"])?;
    let mut stream = df_cols_for_json.clone().execute_stream().await.context("could not create stream")?;
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write_batches(&[&batch])?;
    }
    writer.finish()?;
    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;
    let mut res = HashMap::new();
    for mut json in json_rows {
        // let primary_key = json["id"].clone();
        let primary_key = json.remove("id").unwrap().to_string().parse::<i32>()?;
        // println!("pkey: {:?}", primary_key);
        // println!("json: {:?}", json);
        let m = HashMap::from([(primary_key, json)]);
        res.extend(m);
    }
    println!("res:{:?}", res);

    let mut primary_keys = vec![];
    let mut data_all = vec![];
    for i in res.keys().sorted() {
        // println!("row: {:?}", res[i]);
        primary_keys.push(*i);
        let row = res[i].clone();
        let str_row = serde_json::to_string(&row)?;
        data_all.push(str_row);
    }

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("metadata", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(primary_keys)),
            Arc::new(StringArray::from(data_all)),
        ],
    )?;
    let df_to_json = ctx.read_batch(batch.clone())?;
    df_to_json.clone().show().await?;

    let res = df.join(df_to_json, JoinType::Inner, &["id"], &["id"], None)?;
    res.show().await?;

    Ok(())
}

pub async fn join_dfs_example() -> anyhow::Result<()> {
    let df1 = get_df().await?;
    let df2 = get_df2().await?.with_column_renamed("id", "id_tojoin")?;
    let df = df1.clone().join(df2.clone(), JoinType::Inner, &["id"], &["id_tojoin"], None)?;

    // select all columns from joined df except tojoin
    let columns = df
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .filter(|x| !x.contains("tojoin"))
        .collect::<Vec<_>>();

    let res = df.clone().select_columns(&columns)?;
    res.show().await?;

    Ok(())
}

// add id column to df
pub async fn get_primary_key(col_name: Option<&str>, max: i32) -> anyhow::Result<RecordBatch> {
    let col_name = col_name.unwrap_or("primary_key");
    let data: ArrayRef = Arc::new(Int32Array::from_iter(0..max));
    let record_batch = RecordBatch::try_from_iter(vec![
      (col_name, data),
    ])?;

    Ok(record_batch)
}

pub async fn get_df() -> anyhow::Result<DataFrame> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;

    Ok(df)
}

pub async fn get_df2() -> anyhow::Result<DataFrame> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;

    Ok(df)
}

pub async fn get_df3() -> anyhow::Result<DataFrame> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("pkey", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;

    Ok(df)
}

pub async fn example() -> anyhow::Result<()> {
    // define a schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // define data
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 100])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...)
    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    // filter
    // let df = ctx.
    //     sql("SELECT * FROM t \
    //         WHERE id > 10").await?;        
  
    df.show().await?;

    Ok(())
}

pub async fn join() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![1, 10, 100])),
        ],
    )?;

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["foo", "foo", "baz"])),
            Arc::new(Int32Array::from(vec![1, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("t1", batch1)?;
    ctx.register_batch("t2", batch2)?;
    let df1 = ctx.table("t1").await?;
    let df2 = ctx.table("t2").await?
        .select(vec![
            col("id").alias("id2"),
            col("name").alias("name2")])?;

    let joined = df1
        .join(df2, JoinType::Inner, &["id"], &["id2"], None)?
        .select_columns(&["id", "name", "name2"])?;

    joined.show().await?;
    
    Ok(())
}

pub async fn join_sql() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![1, 10, 100])),
        ],
    )?;

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["foo", "foo", "baz"])),
            Arc::new(Int32Array::from(vec![1, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("t1", batch1)?;
    ctx.register_batch("t2", batch2)?;

    // example how to except the same col id 
    // let res = ctx
    //     .sql("select t1.*, t2.* except(id) \
    //     from t1 inner join t2 \
    //     on t1.id = t2.id").await?;

    let res = ctx
        .sql("select t1.id as id1, t1.name as name1, t2.name as name2 \
            from t1 \
            inner join t2 on t1.id = t2.id \
            where t1.id = 'foo'").await?;

    res.show().await?;
    
    Ok(())
}

pub fn record_batches_to_json_rows() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let a = Int32Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
    let json_rows = arrow::json::writer::record_batches_to_json_rows(&[&batch]).unwrap();
    println!("{:?}", json_rows);
}

pub async fn df_cols_to_json_example() -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    let schema1 = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch1 = RecordBatch::try_new(
        schema1.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;
    let df1 = ctx.read_batch(batch1.clone())?;

    let schema2 = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch2 = RecordBatch::try_new(
        schema2.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df2 = ctx
        .read_batch(batch2.clone())?
        .with_column_renamed("id", "id2")?;

    let df = df1
        .join(df2, JoinType::Inner, &["id"], &["id2"], None)?
        .select_columns(&["id", "name", "data"])?;
    df.clone().show().await?;

    let df_cols_for_json = df.clone().select_columns(&["name", "data", "id"])?;
    // df_cols_for_json.show().await?;
    let mut stream = df_cols_for_json.clone().execute_stream().await.context("could not create stream")?;
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write_batches(&[&batch])?;
    }
    writer.finish()?;
    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;
    let mut res = HashMap::new();
    for mut json in json_rows {
        // let primary_key = json["id"].clone();
        let primary_key = json.remove("id").unwrap().to_string().parse::<i32>()?;
        // println!("pkey: {:?}", primary_key);
        // println!("json: {:?}", json);
        let m = HashMap::from([(primary_key, json)]);
        res.extend(m);
    }
    println!("res:{:?}", res);

    let mut primary_keys = vec![];
    let mut data_all = vec![];
    for i in res.keys().sorted() {
        // println!("row: {:?}", res[i]);
        primary_keys.push(*i);
        let row = res[i].clone();
        let str_row = serde_json::to_string(&row)?;
        data_all.push(str_row);
    }

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("metadata", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(primary_keys)),
            Arc::new(StringArray::from(data_all)),
        ],
    )?;
    let df_to_json = ctx.read_batch(batch.clone())?;
    df_to_json.clone().show().await?;

    let res = df.join(df_to_json, JoinType::Inner, &["id"], &["id"], None)?;
    res.show().await?;

    Ok(())
}

// create json like string column, df should have primary_key with int type
pub async fn df_cols_to_json(ctx: SessionContext, df: DataFrame, cols: &[&str], primary_key: &str, new_col: Option<&str>) -> anyhow::Result<DataFrame> {
    let mut cols_new = cols.iter().map(|x| x.to_owned()).collect::<Vec<_>>();
    cols_new.push(primary_key);

    let df_cols_for_json = df.clone().select_columns(&cols_new)?;
    let mut stream = df_cols_for_json.clone().execute_stream().await.context("could not create stream")?;
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write_batches(&[&batch])?;
    }
    writer.finish()?;
    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;
    let mut res = HashMap::new();
    for mut json in json_rows {
        let primary_key = json.remove(primary_key).unwrap().to_string().parse::<i32>()?;
        res.extend(HashMap::from([(primary_key, json)]));
    }
    // println!("res:{:?}", res);

    let mut primary_keys = vec![];
    let mut data_all = vec![];
    for i in res.keys().sorted() {
        primary_keys.push(*i);
        let row = res[i].clone();
        let str_row = serde_json::to_string(&row)?;
        data_all.push(str_row);
    }

    let mut right_cols = primary_key.to_string();
    right_cols.push_str("tojoin");
    let schema = Schema::new(vec![
        Field::new(right_cols.clone(), DataType::Int32, false),
        Field::new(new_col.unwrap_or("metadata"), DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(primary_keys)),
            Arc::new(StringArray::from(data_all)),
        ],
    )?;
    let df_to_json = ctx.read_batch(batch.clone())?;
    
    let res = df.join(df_to_json, JoinType::Inner, &[primary_key], &[&right_cols], None)?;

    let columns = res
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .filter(|x| !x.contains("tojoin"))
        .filter(|x| cols_new.iter().find(|col| col.contains(x)).is_none())
        .collect::<Vec<_>>();
    // println!("{:?}", columns);

    let res = res.clone().select_columns(&columns)?;

    Ok(res)
}

pub fn df_to_json_str() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let a = Int32Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

    let json_rows = arrow_json::writer::record_batches_to_json_rows(&[&batch]).unwrap();

    assert_eq!(
        serde_json::Value::Object(json_rows[1].clone()),
        serde_json::json!({"a": 2}),
    );

    for json_row in json_rows {
        let value = serde_json::Value::Object(json_row);
        let val_str = serde_json::to_string(&value).unwrap();
        println!("{}", val_str);
    }
}

pub async fn add_col_to_df_example() -> anyhow::Result<()> {
    let ctx = SessionContext::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;
    df.clone().show().await?;

    let new_col = vec!["foo", "bar", "baz"];
    let scalars = new_col.iter().map(|val| ScalarValue::Utf8(Some(val.to_string()))).collect::<Vec<_>>();
    let new_col = ScalarValue::new_list_from_iter(scalars.into_iter(), &DataType::Utf8);
    let _ = df.clone().with_column("new_col1", Expr::Literal(ScalarValue::new_utf8("foo")))?; // add one string for all columns
    let res = df.clone().with_column("new_col2", Expr::Literal(ScalarValue::List(new_col)))?; // add list
    res.show().await?;

    // add column from vec doesn't work
    // https://github.com/apache/arrow-datafusion/pull/9592
    // let new_col = vec!["foo", "bar", "baz"];
    // let exprs = new_col.iter().map(|val| lit(*val)).collect::<Vec<_>>();
    // println!("exprs: {:?}", exprs);
    // let res = df.with_column("new_col", Expr::Unnest(Unnest { exprs }))?;
    // res.show().await?;
    /* 36 version -> 
    Internal error: Unnest should be rewritten to LogicalPlan::Unnest before type coercion.
    This was likely caused by a bug in DataFusion's code and we would welcome that you file an bug report in our issue tracker 
    */
    // 37 version -> Error: Error during planning: unnest() can only be applied to array, struct and null

    Ok(())
}

pub async fn df_struct_example() -> anyhow::Result<()> {
    let ctx = SessionContext::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("id2", DataType::Int32, false),
        Field::new("id3", DataType::Int32, false),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![3])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;

    let boolean = Arc::new(BooleanArray::from(vec![false]));
    let int = Arc::new(Int32Array::from(vec![42]));
    let str = Arc::new(StringArray::from(vec!["foo"]));
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("b", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Int32, false)),
            int.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("d", DataType::Utf8, false)),
            str.clone() as ArrayRef,
        ),
    ]);

    let res = df.with_column("new_col", Expr::Literal(ScalarValue::Struct(struct_array.into())))?;
    res.clone().show().await?;

    Ok(())
}

// doesn't work for batch.len() > 1
pub async fn add_col_to_df_simple() -> anyhow::Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch.clone())?;
    df.show().await?;

    let new_col = vec!["foo", "bar", "baz"]; 
    let schema_new = Schema::new(vec![
        Field::new("new_col", DataType::Utf8, false),
    ]);
    let batch_new = RecordBatch::try_new(
        schema_new.clone().into(),
        vec![
            Arc::new(StringArray::from(new_col)),
        ],
    )?;
    let x = batch.columns().to_vec();
    let y = batch_new.columns().to_vec();
    let mut columns = vec![];
    columns.extend(x);
    columns.extend(y);
    // println!("{:?}", columns);

    let schema_merged = Schema::try_merge(vec![schema, schema_new])?;
    // println!("{:?}", schema_merged);

    let batches = RecordBatch::try_new(schema_merged.into(), columns)?;
    let res = ctx.read_batch(batches)?;

    res.show().await?;

    Ok(())
}

pub async fn get_aws_client(region: &str) -> anyhow::Result<Client> {
    let config = aws_config::defaults(BehaviorVersion::v2023_11_09())
        .region(Region::new(region.to_string()))
        .load()
        .await;

    let client = Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .retry_config(aws_config::retry::RetryConfig::standard()
            .with_max_attempts(10))
            .build()
    );

    Ok(client)
}

pub async fn read_from_s3(ctx: SessionContext, bucket: &str, region: &str, key: &str) -> anyhow::Result<()> {
    let creds = Credentials::default()?;
    let aws_access_key_id = creds.access_key.unwrap();
    let aws_secret_access_key = creds.secret_key.unwrap();
    let aws_session_token = creds.session_token.unwrap();

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(aws_access_key_id)
        .with_secret_access_key(aws_secret_access_key)
        .with_token(aws_session_token)
        .build()?;

    let path = format!("s3://{bucket}");
    let s3_url = Url::parse(&path)?;
    ctx.runtime_env().register_object_store(&s3_url, Arc::new(s3));

    let path = format!("s3://{bucket}/{key}");
    ctx.register_parquet("foo", &path, ParquetReadOptions::default()).await?;
    let df = ctx.sql("select * from foo").await?;
    df.show().await?;

    Ok(())
}

pub async fn write_to_s3(ctx: SessionContext, bucket: &str, region: &str, key: &str, df: DataFrame) -> anyhow::Result<()> {
    let creds = Credentials::default()?;
    let aws_access_key_id = creds.access_key.unwrap();
    let aws_secret_access_key = creds.secret_key.unwrap();
    let aws_session_token = creds.session_token.unwrap();

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(aws_access_key_id)
        .with_secret_access_key(aws_secret_access_key)
        .with_token(aws_session_token)
        .build()?;

    let path = format!("s3://{bucket}");
    let s3_url = Url::parse(&path)?;
    ctx.runtime_env().register_object_store(&s3_url, Arc::new(s3));

    // read from s3 file to df
    // let path = format!("s3://{bucket}/path/to/data/");
    // let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
    // let listing_options = ListingOptions::new(Arc::new(file_format)).with_file_extension(FileType::PARQUET.get_ext());
    // ctx.register_listing_table("foo", &path, listing_options, None, None).await?;
    // let df = ctx.sql("select * from foo").await?;

    let batches = df.collect().await?;
    let df = ctx.read_batches(batches)?;
    let out_path = format!("s3://{bucket}/{key}");
    df.write_parquet(&out_path, DataFrameWriteOptions::new(), None).await.context("could not write to s3")?;

    Ok(())
}

pub async fn write_to_file(df: DataFrame, file_path: &str) -> anyhow::Result<()> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await.context("could not create stream from df")?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None).context("could not create writer")?;
    while let Some(batch) = stream.next().await {
        let batch = batch.context("could not get record batch")?;
        writer.write(&batch).await.context("could not write to writer")?;
    }
    writer.close().await.context("could not close writer")?;

    let mut file = std::fs::File::create(file_path)?;
    file.write_all(&mut buf)?;

    Ok(())
}

pub async fn write_df_to_s3(client: Client, bucket: &str, key: &str, df: DataFrame) -> anyhow::Result<()> {
    let mut buf = vec![];
    // let props = default_builder(&ConfigOptions::default())?.build();
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await.context("could not create stream from df")?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None).context("could not create writer")?;
    while let Some(batch) = stream.next().await {
        let batch = batch.context("could not get record batch")?;
        writer.write(&batch).await.context("could not write to writer")?;
    }
    writer.close().await.context("could not close writer")?;

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context(format!("could not create multipart upload bucket: {} key: {}", bucket, key))?;

    let upload_id = multipart_upload_res.upload_id().context(format!("could not get upload_id for key: {}", key))?;
    let mut upload_parts: Vec<CompletedPart> = Vec::new();
    let mut stream = ByteStream::from(buf);
    let mut part_number = 1;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        let upload_part_res = client
            .upload_part()
            .key(key)
            .bucket(bucket)
            .upload_id(upload_id)
            .body(ByteStream::from(bytes))
            .part_number(part_number)
            .send()
            .await
            .context(format!("could not create upload part for key: {}", key))?;
    
        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );

        part_number += 1;
    }

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await
        .context(format!("could not complete multipart upload for key: {}", key))?;

    Ok(())
}

async fn write_batches_to_s3(client: Client, bucket: &str, key: &str, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
    let mut buf = vec![];
    let schema = batches[0].schema();
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema, None).context("could not create writer")?;
    for batch in batches {
        writer.write(&batch).await.context("could not write to writer")?;
    }
    writer.close().await.context("could not close writer")?;

    let multipart_upload_res: CreateMultipartUploadOutput = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context(format!("could not create multipart upload bucket: {} key: {}", bucket, key))?;

    let upload_id = multipart_upload_res.upload_id().context(format!("could not get upload_id for key: {}", key))?;
    let mut upload_parts: Vec<CompletedPart> = Vec::new();
    let mut stream = ByteStream::from(buf);
    let mut part_number = 1;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        let upload_part_res = client
            .upload_part()
            .key(key)
            .bucket(bucket)
            .upload_id(upload_id)
            .body(ByteStream::from(bytes))
            .part_number(part_number)
            .send()
            .await
            .context(format!("could not create upload part for key: {}", key))?;
    
        upload_parts.push(
            CompletedPart::builder()
                .e_tag(upload_part_res.e_tag.unwrap_or_default())
                .part_number(part_number)
                .build(),
        );

        part_number += 1;
    }

    let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
        .set_parts(Some(upload_parts))
        .build();

    let _complete_multipart_upload_res = client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .multipart_upload(completed_multipart_upload)
        .upload_id(upload_id)
        .send()
        .await
        .context(format!("could not complete multipart upload for key: {}", key))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;

    #[tokio::test]
    async fn test_cols_to_json() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("pkey", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("data", DataType::Int32, true),
        ]);
        let batch = RecordBatch::try_new(
            schema.clone().into(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                Arc::new(Int32Array::from(vec![42, 43, 44])),
            ],
        ).unwrap();
    
        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch.clone()).unwrap();
        let res = df_cols_to_json(ctx, df, &["name", "data"], "pkey", Some("metadata")).await.unwrap();

        assert_eq!(res.schema().fields().len(), 2); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 3); // rows count

        let row1 = res.clone().filter(col("id").eq(lit(1))).unwrap();
        assert_batches_eq!(
            &[
                  "+----+--------------------------+",
                  "| id | metadata                 |",
                  "+----+--------------------------+",
                r#"| 1  | {"data":42,"name":"foo"} |"#,
                  "+----+--------------------------+",
            ],
            &row1.collect().await.unwrap()
        );

        let row2 = res.clone().filter(col("id").eq(lit(2))).unwrap();
        assert_batches_eq!(
            &[
                  "+----+--------------------------+",
                  "| id | metadata                 |",
                  "+----+--------------------------+",
                r#"| 2  | {"data":43,"name":"bar"} |"#,
                  "+----+--------------------------+",
            ],
            &row2.collect().await.unwrap()
        );

        let row3 = res.clone().filter(col("id").eq(lit(3))).unwrap();
        assert_batches_eq!(
            &[
                  "+----+--------------------------+",
                  "| id | metadata                 |",
                  "+----+--------------------------+",
                r#"| 3  | {"data":44,"name":"baz"} |"#,
                  "+----+--------------------------+",
            ],
            &row3.collect().await.unwrap()
        );
    }
}