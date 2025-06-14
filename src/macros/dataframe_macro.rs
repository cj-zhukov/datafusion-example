/// Macro for creating dataframe, similar (almost) to polars
/// # Examples
/// ```
/// # use datafusion_example::df;
/// let df = df!(
///    "id" => [1, 2, 3],
///    "name" => ["foo", "bar", "baz"]
///  );
/// // or using vectors
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
/// let df = df!(); // empty df
/// //++
/// //++
/// ```
#[deprecated]
#[macro_export]
macro_rules! df {
    () => {{
        use std::sync::Arc;

        use datafusion::prelude::*;
        use datafusion::arrow::array::RecordBatch;
        use datafusion::arrow::datatypes::Schema;

        let ctx = SessionContext::new();
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        ctx.read_batch(batch).expect("failed creating empty dataframe")
    }};

    ($($col_name:expr => $data:expr),+ $(,)?) => {{
        use $crate::utils::conversions::{IntoArrayRef, df_from_columns};

        let columns = vec![
            $( ($col_name, Box::new($data) as Box<dyn IntoArrayRef>) ),+
        ];
        df_from_columns(columns).expect("failed to create DataFrame")
    }};
}

/// Macro for creating dataframe, similar (almost) to polars
/// # Examples
/// ```
/// # use datafusion_example::df_try;
/// let df = df_try!(
///    "id" => [1, 2, 3],
///    "name" => ["foo", "bar", "baz"]
///  ).unwrap();
/// // or using vectors
/// let df = df_try!(
///    "id" => vec![1, 2, 3],
///    "name" => vec!["foo", "bar", "baz"]
///  ).unwrap();
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// let df = df_try!().unwrap(); // empty df
/// //++
/// //++
/// ```
#[deprecated]
#[macro_export]
macro_rules! df_try {
    () => {{
        use std::sync::Arc;

        use datafusion::prelude::*;
        use datafusion::arrow::array::RecordBatch;
        use datafusion::arrow::datatypes::Schema;

        let ctx = SessionContext::new();
        let batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        ctx.read_batch(batch)
    }};

    ($($col_name:expr => $data:expr),+ $(,)?) => {{
        use $crate::utils::conversions::{IntoArrayRef, df_from_columns};

        let columns = vec![
            $( ($col_name, Box::new($data) as Box<dyn IntoArrayRef>) ),+
        ];
        df_from_columns(columns)
    }};
}

/// Macro for creating dataframe, similar (almost) to polars
/// # Examples
/// ```
/// # use datafusion_example::dataframe;
/// let df = dataframe!(
///    "id" => vec![1, 2, 3],
///    "name" => vec!["foo", "bar", "baz"]
///  ).unwrap();
/// // +----+------+,
/// // | id | name |,
/// // +----+------+,
/// // | 1  | foo  |,
/// // | 2  | bar  |,
/// // | 3  | baz  |,
/// // +----+------+,
/// let df = dataframe!().unwrap(); // empty df
/// //++
/// //++
/// ```
#[deprecated]
#[macro_export]
macro_rules! dataframe {
    () => {{
        use datafusion::prelude::*;
        use datafusion::arrow::array::RecordBatch;
        use datafusion::arrow::datatypes::Schema;
        use std::sync::Arc;

        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let ctx = SessionContext::new();
        ctx.read_batch(empty_batch)
    }};

    ( $( $name:expr => $data:expr ),+ $(,)? ) => {{
        use datafusion::prelude::*;
        use std::sync::Arc;
        use $crate::utils::conversions::{IntoArrayRefNew, dataframe_from_columns};

        let columns = vec![
            $(
                ($name, $data.into_array_ref_new()),
            )+
        ];

        dataframe_from_columns(columns)
    }};
}