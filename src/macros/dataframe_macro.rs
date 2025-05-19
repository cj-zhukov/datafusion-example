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
/// let df = df!(); // empty df
/// //++
/// //++
/// ```
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
        df_from_columns(columns)
    }};
}
