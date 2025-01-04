use std::sync::Arc;

use arrow_json::ReaderBuilder;
use color_eyre::Result;
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
    // #TODO 
    async fn to_df(ctx: &SessionContext, records: &Vec<Self>) -> Result<DataFrame> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut records = vec![];
    let rec1 = Foo { id: 42, name: "foo".to_string() };
    let rec2 = Foo { id: 43, name: "bar".to_string() };
    let rec3 = Foo { id: 44, name: "baz".to_string() };
    records.push(rec1);
    records.push(rec2);
    records.push(rec3);

    let schema = Foo::schema();
    let mut decoder = ReaderBuilder::new(Arc::new(schema)).build_decoder()?;
    decoder.serialize(&records)?;
    let batch = decoder.flush()?.unwrap();
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;
    df.show().await?;
    Ok(())
}