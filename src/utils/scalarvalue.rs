use std::{str::FromStr, sync::Arc};

use datafusion::arrow::array::{ArrayRef, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Int32Type, UInt32Type};
use datafusion::scalar::ScalarValue;

use crate::error::UtilsError;

/// Converts a single value from an Arrow array into a ScalarValue
pub fn try_from_array(array: &ArrayRef, index: usize) -> Result<ScalarValue, UtilsError> {
    ScalarValue::try_from_array(array, index).map_err(UtilsError::from)
}

/// Parses an iterator of &str into an Arrow ArrayRef of the given DataType.
/// This uses Arrow primitives directly.
///
/// # Examples
/// ```
/// # use datafusion::arrow::array::Int32Array;
/// # use datafusion::arrow::datatypes::DataType;
/// # use datafusion_example::utils::scalarvalue::parse_strings;
/// # use color_eyre::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let array = parse_strings(["1", "2", "3"], DataType::Int32)?;
/// let integers = array.as_any().downcast_ref::<Int32Array>().unwrap();
/// let vals = integers.values();
/// assert_eq!(vals, &[1, 2, 3]);
/// # Ok(())
/// # }
/// ```
pub fn parse_strings<'a, I>(iter: I, to_data_type: DataType) -> Result<ArrayRef, UtilsError>
where
    I: IntoIterator<Item = &'a str>,
{
    match to_data_type {
        DataType::Int32 => {
            let arr = parse_to_primitive::<Int32Type, _>(iter);
            Ok(Arc::new(arr))
        }
        DataType::UInt32 => {
            let arr = parse_to_primitive::<UInt32Type, _>(iter);
            Ok(Arc::new(arr))
        }
        DataType::Utf8 => {
            let vec: Vec<Option<&str>> = iter.into_iter().map(Some).collect();
            Ok(Arc::new(StringArray::from(vec)))
        }
        _ => unimplemented!(),
    }
}

fn parse_to_primitive<'a, T, I>(iter: I) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    T::Native: FromStr,
    I: IntoIterator<Item = &'a str>,
{
    PrimitiveArray::from_iter(iter.into_iter().map(|val| T::Native::from_str(val).ok()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use color_eyre::Result;
    use datafusion::arrow::array::{Array, Int32Array};

    #[test]
    fn test_parse_strings() -> Result<()> {
        let array = parse_strings(["1", "2", "3"], DataType::Int32)?;
        let integers = array.as_any().downcast_ref::<Int32Array>().unwrap();
        let vals = integers.values();
        assert_eq!(vals, &[1, 2, 3]);

        let array = parse_strings(["foo", "bar", "baz"], DataType::Utf8)?;
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        let vals: Vec<_> = (0..strings.len()).map(|i| strings.value(i)).collect();
        assert_eq!(vals, vec!["foo", "bar", "baz"]);

        Ok(())
    }
}
