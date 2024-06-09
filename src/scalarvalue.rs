use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::{Result, anyhow};
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray, PrimitiveArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Int32Type, Schema, UInt32Type};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use tokio_stream::StreamExt;

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

impl ScalarValueNew {
    pub fn try_from_array(array: &ArrayRef, index: usize) -> Result<Self> {
        Ok(match array.data_type() {
            DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean),
            DataType::Int32 => typed_cast!(array, index, Int32Array, Int32),
            DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8),
            other => {
                return Err(anyhow!(format!("Downcast not available for type: {}", other)));
            }
        })
    }
}

pub async fn add_column_with_scalar_new_example() -> Result<()> {
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