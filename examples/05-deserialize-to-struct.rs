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
            Field::new("id", DataType::Int32, false),
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    Foo::example1(&ctx).await?;
    Foo::example2(&ctx).await?;
    Ok(())
}
