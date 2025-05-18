/// Macro for creating dataframe, similar (almost) to polars
/// # Examples
/// ```
/// # use datafusion_example::df;
/// let df = df!(
///    "id" => vec![1, 2, 3],
///    "name" => vec!["foo", "bar", "baz"]
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

    ($($col_name:expr => $data:expr),+ $(,)?) => {{
        use $crate::utils::into_array_ref::{df_from_columns, IntoArrayRef};

        let columns = vec![
            $( ($col_name, Box::new($data) as Box<dyn IntoArrayRef>) ),+
        ];
        df_from_columns(columns)
    }};
}

// /// Macro for creating dataframe, similar (almost) to polars
// /// # Examples
// /// ```
// /// use datafusion::arrow::array::{Int32Array, StringArray};
// /// # use datafusion_example::df;
// /// let id = Int32Array::from(vec![1, 2, 3]);
// /// let name = StringArray::from(vec!["foo", "bar", "baz"]);
// /// let df = df!(
// ///    "id" => id,
// ///    "name" => name
// ///  );
// /// // +----+------+,
// /// // | id | name |,
// /// // +----+------+,
// /// // | 1  | foo  |,
// /// // | 2  | bar  |,
// /// // | 3  | baz  |,
// /// // +----+------+,
// /// ```
// #[macro_export]
// macro_rules! df {
//     () => {{
//         use datafusion::prelude::*;

//         let ctx = SessionContext::new();
//         ctx.read_empty().expect("failed creating empty dataframe")
//     }};

//     ($($col_name:expr => $data:expr),+) => {{
//         use datafusion::prelude::*;
//         use datafusion::arrow::array::{RecordBatch, ArrayRef};
//         use datafusion::arrow::datatypes::{Field, Schema};
//         use std::sync::Arc;

//         let mut fields = vec![];
//         let mut columns: Vec<ArrayRef> = vec![];
//         $(
//             let col = Arc::new($data) as ArrayRef;
//             let dtype = col.data_type().clone();
//             fields.push(Field::new($col_name, dtype, true));
//             columns.push(col);
//         )+
//         let len = columns[0].len();
//         debug_assert!(
//             columns.iter().all(|col| col.len() == len),
//             "all columns must have the same length"
//         );

//         let schema = Arc::new(Schema::new(fields));
//         let batch = RecordBatch::try_new(schema.clone(), columns)
//             .expect(&format!("failed creating batch with schema: {:?}", schema));
//         let ctx = SessionContext::new();
//         ctx.read_batch(batch).expect("failed creating dataframe")
//     }};
// }