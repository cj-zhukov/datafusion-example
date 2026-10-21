use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;

// 1. Your own plain Rust data. Nothing Arrow-related here.
#[derive(Debug, Clone)]
struct User {
    id: i32,
    name: String,
}

// 2. The provider: a struct that *owns* (or knows how to reach) the data.
#[derive(Debug)]
struct UsersTable {
    users: Vec<User>,
    schema: SchemaRef, // computed once, handed out many times
}

impl UsersTable {
    fn new(users: Vec<User>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        Self { users, schema }
    }

    // Helper: convert Vec<User> (row-oriented) -> RecordBatch (column-oriented).
    fn to_batch(&self) -> Result<RecordBatch> {
        let ids = Int32Array::from_iter_values(self.users.iter().map(|u| u.id));
        let names = StringArray::from_iter_values(self.users.iter().map(|u| u.name.as_str()));
        Ok(RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(ids), Arc::new(names)],
        )?)
    }
}

// 3. The trait. Only three methods are required.
#[async_trait]
impl TableProvider for UsersTable {
    // Planning-time question: "what columns and types do you have?"
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    // Is this a real table, a view, or a temp table? Affects things like SHOW TABLES.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    // Execution-time question: "give me a physical plan that yields rows."
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch = self.to_batch()?;

        // MemorySourceConfig is a ready-made ExecutionPlan that just emits
        // the batches you give it. It also applies the projection for us.
        MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            self.schema(),
            projection.map(|p| p.to_vec()),
        )
        .map(|exec| exec as Arc<dyn ExecutionPlan>)
    }
}

pub async fn custom_table_provider_example() -> Result<()> {
    let table = UsersTable::new(vec![
        User {
            id: 1,
            name: "Alice".into(),
        },
        User {
            id: 2,
            name: "Bob".into(),
        },
        User {
            id: 3,
            name: "Carol".into(),
        },
    ]);

    let ctx = SessionContext::new();
    ctx.register_table("users", Arc::new(table))?;

    let df = ctx
        .sql("SELECT name FROM users WHERE id > 1 ORDER BY name")
        .await?;
    df.show().await?;
    // +-------+
    // | name  |
    // +-------+
    // | Bob   |
    // | Carol |
    // +-------+
    Ok(())
}
