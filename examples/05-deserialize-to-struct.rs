use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{AsArray, BinaryArray, Int32Array, RecordBatch, StringArray};
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
    pub async fn new() -> Result<()> {
        let ctx = SessionContext::new();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("data", DataType::Binary, true),
        ]);
        let batch = RecordBatch::try_new(
            schema.clone().into(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                Arc::new(BinaryArray::from_vec(vec![b"foo", b"bar", b"baz"])),
            ],
        )?;
        let df = ctx.read_batch(batch.clone())?;

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
}

#[tokio::main]
async fn main() -> Result<()> {
    Foo::new().await?;
    Ok(())
}
