use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::prelude::*;

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

/// Helper for creating dataframe
/// # Examples
/// ```
/// # use datafusion_example::utils::conversions::df_from_columns;
/// let id = Box::new(vec![1, 2, 3]);
/// let name = Box::new(vec!["foo", "bar", "baz"]);
/// let df = df_from_columns(vec![("id", id), ("name", name)]);
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// ```
pub fn df_from_columns(columns: Vec<(&str, Box<dyn IntoArrayRef>)>) -> DataFrame {
    let mut fields = vec![];
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
    let batch = RecordBatch::try_new(schema.clone(), arrays).expect("failed creating batch");
    let ctx = SessionContext::new();
    ctx.read_batch(batch).expect("failed reading batch")
}
