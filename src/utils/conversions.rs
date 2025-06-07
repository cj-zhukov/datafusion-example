use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::prelude::*;

use datafusion::common::create_array;
use arrow::array::Array;

use crate::error::UtilsError;

/// Converts a vector into an ArrayRef
pub trait IntoArrayRef {
    fn into_array_ref(self: Box<Self>) -> ArrayRef;
}

impl IntoArrayRef for Vec<i32> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Int32Array::from(*self))
    }
}

impl IntoArrayRef for Vec<Option<i32>> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Int32Array::from(*self))
    }
}

impl<const N: usize> IntoArrayRef for [i32; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Int32Array::from(Vec::from(*self)))
    }
}

impl<const N: usize> IntoArrayRef for [Option<i32>; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(Int32Array::from(Vec::from(*self)))
    }
}

impl IntoArrayRef for Vec<&str> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(*self))
    }
}

impl IntoArrayRef for Vec<Option<&str>> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(*self))
    }
}

impl<const N: usize> IntoArrayRef for [&'static str; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(Vec::from(*self)))
    }
}

impl<const N: usize> IntoArrayRef for [Option<&'static str>; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(StringArray::from(Vec::from(*self)))
    }
}

impl IntoArrayRef for Vec<bool> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(BooleanArray::from(*self))
    }
}

impl IntoArrayRef for Vec<Option<bool>> {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(BooleanArray::from(*self))
    }
}

impl<const N: usize> IntoArrayRef for [bool; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(BooleanArray::from(Vec::from(*self)))
    }
}

impl<const N: usize> IntoArrayRef for [Option<bool>; N] {
    fn into_array_ref(self: Box<Self>) -> ArrayRef {
        Arc::new(BooleanArray::from(Vec::from(*self)))
    }
}

// -- new
pub trait IntoArrayRefNew {
    fn into_array_ref_new(self) -> ArrayRef;
}
// -- i32
impl IntoArrayRefNew for Vec<i32> {
    fn into_array_ref_new(self) -> ArrayRef {
        create_array!(Int32, self)
    }
}

impl IntoArrayRefNew for Vec<Option<i32>> {
    fn into_array_ref_new(self) -> ArrayRef {
        create_array!(Int32, self)
    }
}

impl IntoArrayRefNew for &[i32] {
    fn into_array_ref_new(self) -> ArrayRef {
        create_array!(Int32, self.to_vec())
    }
}

impl IntoArrayRefNew for &[Option<i32>] {
    fn into_array_ref_new(self) -> ArrayRef {
        create_array!(Int32, self.to_vec())
    }
}
// -- str
impl IntoArrayRefNew for Vec<&str> {
    fn into_array_ref_new(self) -> ArrayRef {
        create_array!(Utf8, self)
    }
}

impl IntoArrayRefNew for Vec<Option<&str>> {
    fn into_array_ref_new(self) -> ArrayRef {
        create_array!(Utf8, self)
    }
}

impl<const N: usize> IntoArrayRefNew for [&str; N] {
    fn into_array_ref_new(self) -> ArrayRef {
        create_array!(Utf8, self.to_vec())
    }
}

/// Helper for creating dataframe
/// # Examples
/// ```
/// # use datafusion_example::utils::conversions::df_from_columns;
/// let id = Box::new([1, 2, 3]); 
/// let name = Box::new(["foo", "bar", "baz"]);
/// let df = df_from_columns(vec![("id", id), ("name", name)]).unwrap();
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// ```
pub fn df_from_columns(columns: Vec<(&str, Box<dyn IntoArrayRef>)>) -> Result<DataFrame, UtilsError> {
    let mut fields: Vec<Field> = vec![];
    let mut arrays: Vec<ArrayRef> = vec![];

    for (name, array_like) in columns {
        let array = array_like.into_array_ref();
        let dtype = array.data_type().clone();
        fields.push(Field::new(name, dtype, true));
        arrays.push(array);
    }
    let len = arrays[0].len();
    debug_assert!(
        arrays.iter().all(|arr| arr.len() == len),
        "all columns must have the same length"
    );

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;
    Ok(df)
}

/// Helper for creating dataframe
/// # Examples
/// ```
/// # use datafusion_example::utils::conversions::dataframe_from_columns;
/// use std::sync::Arc;
/// use arrow::array::{Int32Array, StringArray, ArrayRef};
/// let id: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3])); 
/// let name: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));
/// let df = dataframe_from_columns(vec![("id", id), ("name", name)]).unwrap();
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// ```
pub fn dataframe_from_columns(
    columns: Vec<(&str, ArrayRef)>
) -> Result<DataFrame, UtilsError> {
    let fields = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect::<Vec<_>>();

    let arrays = columns
        .into_iter()
        .map(|(_, array)| array)
        .collect::<Vec<_>>();

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)?;
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;
    Ok(df)
}
