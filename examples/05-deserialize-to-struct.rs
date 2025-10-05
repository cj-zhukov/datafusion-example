use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{Array, AsArray, BinaryArray, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::prelude::*;
use itertools::izip;
use tokio_stream::StreamExt;

#[derive(Debug)]
pub struct Foo {
    pub id: Option<i32>,
    pub name: Option<String>,
    pub data_val: Option<Vec<u8>>,
}

impl Foo {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("data", DataType::Binary, true),
        ])
    }

    pub fn data() -> Vec<Arc<dyn Array>> {
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(BinaryArray::from_vec(vec![b"foo", b"bar", b"baz"])),
        ]
    }

    pub fn data_with_null() -> Vec<Arc<dyn Array>> {
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None])),
            Arc::new(StringArray::from(vec![Some("foo"), None, None])),
            Arc::new(BinaryArray::from_opt_vec(vec![Some(b"foo"), None, None])),
        ]
    }
}

impl Foo {
    pub async fn example1(ctx: &SessionContext) -> Result<()> {
        let schema = Self::schema();
        let data = Self::data();
        let batch = RecordBatch::try_new(Arc::new(schema), data)?;
        let df = ctx.read_batch(batch)?;

        let mut stream = df.execute_stream().await?;
        let mut records = vec![];
        while let Some(batch) = stream.next().await.transpose()? {
            let ids = batch.column(0).as_primitive::<Int32Type>();
            let names = batch.column(1).as_string::<i32>();
            let data_vals = batch.column(2).as_binary::<i32>();

            for (id, name, data_val) in izip!(ids, names, data_vals) {
                let name = name.map(|x| String::from_utf8(x.into()).unwrap());
                let data_val = data_val.map(|x| x.to_vec());

                records.push(Foo { id, name, data_val });
            }
        }

        println!("{:?}", records);
        Ok(())
    }

    pub async fn example2(ctx: &SessionContext) -> Result<()> {
        let schema = Self::schema();
        let data = Self::data();
        let batch = RecordBatch::try_new(Arc::new(schema), data)?;
        let df = ctx.read_batch(batch)?;

        let mut stream = df.execute_stream().await?;
        let mut records = vec![];
        while let Some(batch) = stream.next().await.transpose()? {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let data_vals = batch
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                let id = if ids.is_null(i) {
                    None
                } else {
                    Some(ids.value(i))
                };

                let name = if names.is_null(i) {
                    None
                } else {
                    Some(names.value(i).to_string())
                };

                let data_val = if data_vals.is_null(i) {
                    None
                } else {
                    Some(data_vals.value(i).to_vec())
                };

                records.push(Self { id, name, data_val });
            }
        }

        println!("{:?}", records);
        Ok(())
    }

    /// shows how to deserialize dataframe to struct, if some columns don't exist in dataframe
    pub async fn example3(ctx: &SessionContext) -> Result<()> {
        let schema = Self::schema();
        let data = Self::data_with_null();
        let batch = RecordBatch::try_new(Arc::new(schema), data)?;
        let df = ctx.read_batch(batch)?;

        let mut stream = df.execute_stream().await?;
        let mut records = vec![];
        while let Some(batch) = stream.next().await.transpose()? {
            let schema = batch.schema();
            let columns = schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<_>>();

            // helper closure to get optional StringArray
            let get_string_col = |name: &str| -> Option<&StringArray> {
                columns
                    .iter()
                    .position(|n| n == name)
                    .map(|i| batch.column(i).as_any().downcast_ref::<StringArray>())
                    .flatten()
            };

            // helper closure for Int32Array
            let get_int_col = |name: &str| -> Option<&Int32Array> {
                columns
                    .iter()
                    .position(|n| n == name)
                    .map(|i| batch.column(i).as_any().downcast_ref::<Int32Array>())
                    .flatten()
            };

            // helper closure for BinaryArray
            let get_bin_col = |name: &str| -> Option<&BinaryArray> {
                columns
                    .iter()
                    .position(|n| n == name)
                    .map(|i| batch.column(i).as_any().downcast_ref::<BinaryArray>())
                    .flatten()
            };

            let ids = get_int_col("id");
            let names = get_string_col("name");
            let data_vals = get_bin_col("data");

            for i in 0..batch.num_rows() {
                records.push(Self {
                    id: ids.and_then(|col| if col.is_null(i) { None } else { Some(col.value(i)) }),
                    name: names.and_then(|col| if col.is_null(i) { None } else { Some(col.value(i).to_string()) }),
                    data_val: data_vals.and_then(|col| if col.is_null(i) { None } else { Some(col.value(i).to_vec()) }),
                });
            }
        }

        println!("{:?}", records);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    Foo::example1(&ctx).await?;
    Foo::example2(&ctx).await?;
    Foo::example3(&ctx).await?;
    Ok(())
}
