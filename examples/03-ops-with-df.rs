use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, AsArray, Float32Array, Int32Array, ListArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Float32Type, Int32Type, Schema};
use datafusion::arrow::compute::concat;
use datafusion::{arrow, assert_batches_eq, prelude::*};
use datafusion::scalar::ScalarValue;
use datafusion_example::utils::scalarvalue::parse_strings;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    join().await?;
    join_sql().await?;
    add_literal_col().await?;
    add_str_col().await?;
    assert1().await?;
    assert2().await?;
    downcast_df().await?;
    downcast_df2();

    Ok(())
}

pub async fn join() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![1, 10, 100])),
        ],
    )?;

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["foo", "foo", "baz"])),
            Arc::new(Int32Array::from(vec![1, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("t1", batch1)?;
    ctx.register_batch("t2", batch2)?;
    let df1 = ctx.table("t1").await?;
    let df2 = ctx.table("t2").await?
        .select(vec![
            col("id").alias("id2"),
            col("name").alias("name2")])?;

    let joined = df1
        .join(df2, JoinType::Inner, &["id"], &["id2"], None)?
        .select_columns(&["id", "name", "name2"])?;

    joined.show().await?;
    
    Ok(())
}

pub async fn join_sql() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Int32, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![1, 10, 100])),
        ],
    )?;

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["foo", "foo", "baz"])),
            Arc::new(Int32Array::from(vec![1, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("t1", batch1)?;
    ctx.register_batch("t2", batch2)?;

    let res = ctx
        .sql("select t1.id as id1, t1.name as name1, t2.name as name2 \
            from t1 \
            inner join t2 on t1.id = t2.id \
            where t1.id = 'foo'").await?;

    res.show().await?;
    
    Ok(())
}

pub async fn add_literal_col() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch.clone())?;

    let res = df.with_column("new_col1", Expr::Literal(ScalarValue::from("foo")))?;
    let res = res.with_column("new_col2", Expr::Literal(ScalarValue::from(42)))?;

    res.show().await?;

    Ok(())
}

pub async fn add_str_col() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch.clone())?;

    let schema = df.schema().clone();
    let batches = df.collect().await?;
    let batches = batches.iter().collect::<Vec<_>>();
    let field_num = schema.fields().len();
    let mut arrays = Vec::with_capacity(field_num);
    for i in 0..field_num {
        let array = batches
            .iter()
            .map(|batch| batch.column(i).as_ref())
            .collect::<Vec<_>>();
        let array = concat(&array)?;
        arrays.push(array);
    }
    
    let new_col = vec!["foo", "bar", "baz"];
    let new_col: ArrayRef = Arc::new(StringArray::from(new_col));
    arrays.push(new_col);
    let schema_new_col = Schema::new(vec![Field::new("col_name", DataType::Utf8, true)]);

    let schema_new = Schema::try_merge(vec![schema.as_arrow().clone(), schema_new_col])?;
    let batch = RecordBatch::try_new(schema_new.into(), arrays)?;
    let res = ctx.read_batch(batch)?;

    res.show().await?;

    Ok(())
}

pub async fn assert1() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch.clone())?;
    let rows_count = df.clone().count().await?;
    let columns_count = df.clone().schema().field_names().len();
    assert_eq!(rows_count, 3);
    assert_eq!(columns_count, 2);

    let results: Vec<RecordBatch> = df.collect().await?;
    let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?.to_string();
    let expected = vec![
        "+----+------+",
        "| id | data |",
        "+----+------+",
        "| 1  | 42   |",
        "| 2  | 43   |",
        "| 3  | 44   |",
        "+----+------+",
    ];
    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

    Ok(())
}

pub async fn assert2() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;

    let ctx = SessionContext::new();
    let res = ctx.read_batch(batch.clone())?;

    assert_eq!(res.schema().fields().len(), 3); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 3); // rows count

    // check all df
    let rows = res.clone().sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | data | name |",
            "+----+------+------+",
            "| 1  | 42   | foo  |",
            "| 2  | 43   | bar  |",
            "| 3  | 44   | baz  |",
            "+----+------+------+",
        ],
        &rows.collect().await.unwrap()
    );

    // check each row
    let row1 = res.clone().filter(col("id").eq(lit(1)))?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | data | name |",
            "+----+------+------+",
            "| 1  | 42   | foo  |",
            "+----+------+------+",
        ],
        &row1.collect().await.unwrap()
    );

    let row2 = res.clone().filter(col("id").eq(lit(2)))?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | data | name |",
            "+----+------+------+",
            "| 2  | 43   | bar  |",
            "+----+------+------+",
        ],
        &row2.collect().await.unwrap()
    );

    let row3 = res.clone().filter(col("id").eq(lit(3)))?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | data | name |",
            "+----+------+------+",
            "| 3  | 44   | baz  |",
            "+----+------+------+",
        ],
        &row3.collect().await.unwrap()
    );

    Ok(())
}

pub async fn downcast_df() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Float32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Float32Array::from(vec![42.0, 43.0, 44.0])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;

    let mut stream = df.execute_stream().await?;
    let mut id_vals = vec![];
    let mut name_vals = vec![];
    let mut data_vals = vec![];
    while let Some(batch) = stream.next().await.transpose()? {
        let ids = batch.column(0).as_primitive::<Int32Type>();
        for id in ids {
            id_vals.push(id);
        }

        let names = batch.column(1).as_string::<i32>();
        for name in names {
            let name = name.map(|x| x.to_string());
            name_vals.push(name);
        }

        let data_all = batch.column(2).as_primitive::<Float32Type>();
        for data in data_all {
            data_vals.push(data);
        }
    }
    println!("{:?}", id_vals);
    println!("{:?}", name_vals);
    println!("{:?}", data_vals);

    Ok(())
}

pub fn downcast_df2() {
    let array = parse_strings(["1", "2", "3"], DataType::Int32);
    let integers = array.as_any().downcast_ref::<Int32Array>().unwrap();
    let vals = integers.values();
    assert_eq!(vals, &[1, 2, 3]);

    let scalars = vec![
        ScalarValue::Int32(Some(1)),
        ScalarValue::Int32(None),
        ScalarValue::Int32(Some(2))
    ];
    let result = ScalarValue::new_list_from_iter(scalars.into_iter(), &DataType::Int32, true);
    let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(
        vec![
        Some(vec![Some(1), None, Some(2)])
        ]);

    assert_eq!(*result, expected);
}