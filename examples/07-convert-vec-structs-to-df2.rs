use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;

pub struct Foo {
    pub id: Option<i32>,
    pub name: Option<String>,
}

fn get_foos() -> Vec<Foo> {
    let mut records = vec![];
    let rec1 = Foo {
        id: Some(42),
        name: Some("foo".to_string()),
    };
    let rec2 = Foo {
        id: Some(43),
        name: Some("bar".to_string()),
    };
    let rec3 = Foo {
        id: Some(44),
        name: Some("baz".to_string()),
    };
    records.push(rec1);
    records.push(rec2);
    records.push(rec3);
    records
}

impl Foo {
    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, true), 
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn to_record_batch(records: &[Self]) -> Result<RecordBatch> {
        let schema = Foo::schema();
        let ids = records.iter().map(|r| r.id).collect::<Vec<_>>();
        let names = records.iter().map(|r| r.name.as_deref()).collect::<Vec<_>>();

        Ok(RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )?)
    }
}

impl Foo {
    pub async fn to_df(
        ctx: &SessionContext, 
        records: &[Self],
    ) -> Result<DataFrame> {
        let batch = Self::to_record_batch(records)?;
        let df = ctx.read_batch(batch)?;
        Ok(df)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let records = get_foos();
    let ctx = SessionContext::new();
    let df = Foo::to_df(&ctx, &records).await?;
    df.show().await?;
    Ok(())
}