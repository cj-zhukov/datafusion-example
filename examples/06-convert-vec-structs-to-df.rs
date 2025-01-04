use std::sync::Arc;

use arrow_json::ReaderBuilder;
use color_eyre::Result;
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use serde::Serialize;

#[derive(Serialize)]
pub struct Foo {
    pub id: i32,
    pub name: String, 
}

impl Foo {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }
}

impl Foo {
    async fn to_df(ctx: &SessionContext, records: &Vec<Self>) -> Result<DataFrame> {
        let mut ids = vec![];
        let mut names = vec![];

        for record in records {
            ids.push(record.id);
            names.push(record.name.as_ref());
        }

        let batch = RecordBatch::try_new(
            Arc::new(Foo::schema()),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )?;

        let df = ctx.read_batch(batch)?;
        Ok(df)
    }
}

fn get_foos() -> Vec<Foo> {
    let mut records = vec![];
    let rec1 = Foo { id: 42, name: "foo".to_string() };
    let rec2 = Foo { id: 43, name: "bar".to_string() };
    let rec3 = Foo { id: 44, name: "baz".to_string() };
    records.push(rec1);
    records.push(rec2);
    records.push(rec3);
    records
}

#[tokio::main]
async fn main() -> Result<()> {
    let records = get_foos();
    let ctx = SessionContext::new();

    convert_vec_structs_to_df1(&ctx, &records).await?;
    convert_vec_structs_to_df2(&ctx, &records).await?;
    Ok(())
}

async fn convert_vec_structs_to_df1(ctx: &SessionContext, records: &Vec<Foo>) -> Result<()> {
    let schema = Foo::schema();
    let mut decoder = ReaderBuilder::new(Arc::new(schema)).build_decoder()?;
    decoder.serialize(records)?;
    let batch = decoder.flush()?.unwrap();
    let df = ctx.read_batch(batch)?;
    df.show().await?;
    Ok(())
}

async fn convert_vec_structs_to_df2(ctx: &SessionContext, records: &Vec<Foo>) -> Result<()> {
    let df = Foo::to_df(ctx, records).await?;
    df.show().await?;
    Ok(())
}