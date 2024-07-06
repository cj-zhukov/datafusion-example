use std::{str::FromStr, sync::Arc};

use anyhow::{Result, anyhow};
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int32Array, ListArray, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Int32Type, UInt32Type};
use datafusion::scalar::ScalarValue;

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