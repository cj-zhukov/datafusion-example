use crate::df_cols_to_json;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::compute::concat;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch, StringArray, StructArray};
use datafusion::scalar::ScalarValue;
use datafusion::{arrow, assert_batches_eq, prelude::*};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
use itertools::Itertools;
use serde_json::{Map, Value};
use tokio_stream::StreamExt;

pub async fn simple() -> Result<()> {
    // define a schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // define data
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 100])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...)
    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    // filter
    // let df = ctx.
    //     sql("SELECT * FROM t \
    //         WHERE id > 10").await?;        
  
    df.show().await?;

    Ok(())
}

pub fn get_df() -> Result<DataFrame> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;

    Ok(df)
}

pub fn get_df2() -> Result<DataFrame> {
    let ctx = SessionContext::new();
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
    let df = ctx.read_batch(batch.clone())?;

    Ok(df)
}

pub fn get_df3() -> Result<DataFrame> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("pkey", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![None, None, Some("baz")])),
            Arc::new(Int32Array::from(vec![None, None, Some(44)])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;

    Ok(df)
}

pub fn get_df4() -> Result<DataFrame> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("foo", DataType::Utf8, true),
        Field::new("bar", DataType::Utf8, true),
        Field::new("baz", DataType::Utf8, true),
    ]);
    
    let id: Int32Array = Int32Array::from_iter(0..1_000_000 as i32);
    let foo: StringArray = std::iter::repeat(Some("foo")).take(1_000_000).collect();
    let bar: StringArray = std::iter::repeat(Some("bar")).take(1_000_000).collect();
    let baz: StringArray = std::iter::repeat(Some("baz")).take(1_000_000).collect();

    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(id),
            Arc::new(foo),
            Arc::new(bar),
            Arc::new(baz),
        ],
    )?;

    let df = ctx.read_batch(batch.clone())?;

    Ok(df)
}

pub fn create_df_with_serial_col() -> Result<DataFrame> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("pkey", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from_iter(0..3_i32)),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;

    Ok(df)
}

pub async fn assert_example_simple() -> Result<()> {
    let ctx = SessionContext::new();

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
    let df = ctx.read_batch(batch.clone())?;
    df.clone().show().await?;
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

pub async fn assert_example() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    ).unwrap();

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch.clone()).unwrap();
    let res = df_cols_to_json(ctx, df, &["name", "data"], Some("metadata")).await.unwrap();

    assert_eq!(res.schema().fields().len(), 2); // columns count
    assert_eq!(res.clone().count().await.unwrap(), 3); // rows count

    // check all df
    let rows = res.clone().sort(vec![col("id").sort(true, true)]).unwrap();
    assert_batches_eq!(
        &[
              "+----+--------------------------+",
              "| id | metadata                 |",
              "+----+--------------------------+",
            r#"| 1  | {"data":42,"name":"foo"} |"#,
            r#"| 2  | {"data":43,"name":"bar"} |"#,
            r#"| 3  | {"data":44,"name":"baz"} |"#,
              "+----+--------------------------+",
        ],
        &rows.collect().await.unwrap()
    );

    // check each row
    let row1 = res.clone().filter(col("id").eq(lit(1))).unwrap();
    assert_batches_eq!(
        &[
              "+----+--------------------------+",
              "| id | metadata                 |",
              "+----+--------------------------+",
            r#"| 1  | {"data":42,"name":"foo"} |"#,
              "+----+--------------------------+",
        ],
        &row1.collect().await.unwrap()
    );

    let row2 = res.clone().filter(col("id").eq(lit(2))).unwrap();
    assert_batches_eq!(
        &[
              "+----+--------------------------+",
              "| id | metadata                 |",
              "+----+--------------------------+",
            r#"| 2  | {"data":43,"name":"bar"} |"#,
              "+----+--------------------------+",
        ],
        &row2.collect().await.unwrap()
    );

    let row3 = res.clone().filter(col("id").eq(lit(3))).unwrap();
    assert_batches_eq!(
        &[
              "+----+--------------------------+",
              "| id | metadata                 |",
              "+----+--------------------------+",
            r#"| 3  | {"data":44,"name":"baz"} |"#,
              "+----+--------------------------+",
        ],
        &row3.collect().await.unwrap()
    );
}

pub async fn join_dfs_example() -> Result<()> {
    let df1 = get_df()?;
    let df2 = get_df2()?.with_column_renamed("id", "id_tojoin")?;
    let df = df1.clone().join(df2.clone(), JoinType::Inner, &["id"], &["id_tojoin"], None)?;

    // select all columns from joined df except tojoin
    let columns = df
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .filter(|x| !x.contains("tojoin"))
        .collect::<Vec<_>>();

    let res = df.clone().select_columns(&columns)?;
    res.show().await?;

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

    // example how to except the same col id 
    // let res = ctx
    //     .sql("select t1.*, t2.* except(id) \
    //     from t1 inner join t2 \
    //     on t1.id = t2.id").await?;

    let res = ctx
        .sql("select t1.id as id1, t1.name as name1, t2.name as name2 \
            from t1 \
            inner join t2 on t1.id = t2.id \
            where t1.id = 'foo'").await?;

    res.show().await?;
    
    Ok(())
}

pub async fn demo_struct() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;

    let df_to_struct = df.clone().select_columns(&["id", "name", "data"])?;
    let batches = df_to_struct.collect().await?;
    let mut ids = vec![];
    let mut names = vec![];
    let mut data_all = vec![];
    for batch in &batches {
        let xs = batch.columns();
        let id = xs.first().unwrap().as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>();
        let name = xs.get(1).unwrap().as_any().downcast_ref::<StringArray>().unwrap().iter().collect::<Vec<_>>();
        let data = xs.get(2).unwrap().as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>();
        ids.extend(id);
        names.extend(name);
        data_all.extend(data);
    }

    let schema = Schema::new(vec![
        Field::new("id2", DataType::Int32, false),
        Field::new("metadata", DataType::Struct(Fields::from(vec![
                Field::new("name", DataType::Utf8, false), 
                Field::new("data", DataType::Int32, false),
            ])), false),
        ]
    );
    let ids = Int32Array::from(ids);
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, false)), 
            Arc::new(StringArray::from(names)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("data", DataType::Int32, false)), 
            Arc::new(Int32Array::from(data_all)) as ArrayRef,
        ),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(ids),
            Arc::new(struct_array),
        ],
    )?;
    let df_struct = ctx.read_batch(batch)?;

    let res = df
        .join(df_struct, JoinType::Inner, &["id"], &["id2"], None)?
        .select_columns(&["id", "metadata"])?;

    res.show().await?;

    Ok(())
}

pub fn record_batches_to_json_rows() -> Result<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let a = Int32Array::from(vec![1, 2, 3]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();
    
    let buf = vec![];
    let mut writer = arrow_json::ArrayWriter::new(buf);
    writer.write(&batch).unwrap();
    writer.finish()?;

    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;
    println!("{:?}", json_rows);

    for json_row in json_rows {
        let value = serde_json::Value::Object(json_row);
        let val_str = serde_json::to_string(&value).unwrap();
        println!("{}", val_str);
    }

    Ok(())
}

pub async fn df_cols_to_json_example() -> Result<()> {
    let ctx = SessionContext::new();
    let schema1 = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch1 = RecordBatch::try_new(
        schema1.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;
    let df1 = ctx.read_batch(batch1.clone())?;

    let schema2 = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch2 = RecordBatch::try_new(
        schema2.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df2 = ctx
        .read_batch(batch2.clone())?
        .with_column_renamed("id", "id2")?;

    let df = df1
        .join(df2, JoinType::Inner, &["id"], &["id2"], None)?
        .select_columns(&["id", "name", "data"])?;
    df.clone().show().await?;

    let df_cols_for_json = df.clone().select_columns(&["name", "data", "id"])?;
    // df_cols_for_json.show().await?;
    let mut stream = df_cols_for_json.clone().execute_stream().await?;
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write_batches(&[&batch])?;
    }
    writer.finish()?;
    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;
    let mut res = HashMap::new();
    for mut json in json_rows {
        // let primary_key = json["id"].clone();
        let primary_key = json.remove("id").unwrap().to_string().parse::<i32>()?;
        // println!("pkey: {:?}", primary_key);
        // println!("json: {:?}", json);
        let m = HashMap::from([(primary_key, json)]);
        res.extend(m);
    }
    println!("res:{:?}", res);

    let mut primary_keys = vec![];
    let mut data_all = vec![];
    for i in res.keys().sorted() {
        // println!("row: {:?}", res[i]);
        primary_keys.push(*i);
        let row = res[i].clone();
        let str_row = serde_json::to_string(&row)?;
        data_all.push(str_row);
    }

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("metadata", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(primary_keys)),
            Arc::new(StringArray::from(data_all)),
        ],
    )?;
    let df_to_json = ctx.read_batch(batch.clone())?;
    df_to_json.clone().show().await?;

    let res = df.join(df_to_json, JoinType::Inner, &["id"], &["id"], None)?;
    res.show().await?;

    Ok(())
}

pub async fn add_col_to_df_simple() -> Result<()> {
    let ctx = SessionContext::new();

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
    let df = ctx.read_batch(batch.clone())?;
    df.clone().show().await?;

    let res = df.with_column("new_col1", Expr::Literal(ScalarValue::from("foo")))?;
    res.clone().show().await?;

    let res = res.with_column("new_col2", Expr::Literal(ScalarValue::from(42)))?;
    res.clone().show().await?;

    Ok(())
}

// add col of vec<&str> to existing df
pub async fn add_str_col_to_df_example() -> Result<()> {
    let ctx = SessionContext::new();

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
    let df = ctx.read_batch(batch.clone())?;
    df.clone().show().await?;

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

pub async fn df_list_arr_example() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
        Field::new("list", DataType::List(Arc::new(Field::new("item", DataType::Int32, true))), true),
    ]);

    let my_list_data = vec![
        Some(vec![Some(0), Some(1), Some(2)]),
        None,
        Some(vec![Some(6), Some(7)]),
    ];
    let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(my_list_data);

    let batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
            Arc::new(list_array),
        ],
    )?;

    let res = ctx.read_batch(batch)?;
    res.show().await?;

    Ok(())
}

pub async fn df_struct_example1() -> Result<DataFrame> {
    let ctx = SessionContext::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("metadata", DataType::Struct(Fields::from(vec![
                Field::new("name", DataType::Utf8, false), 
                Field::new("data", DataType::Int32, false),
                Field::new("new", DataType::Int32, false)
            ])), false),
        ]
    );

    let ids = Int32Array::from(vec![1, 2, 3]);

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("data", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("new", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![42, 43, 44])) as ArrayRef,
        ),
    ]);

    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(ids),
            Arc::new(struct_array),
        ],
    )?;

    let res = ctx.read_batch(batch)?;

    Ok(res)
}

pub async fn df_struct_example2() -> Result<()> {
    let ctx = SessionContext::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;

    let data1 = Arc::new(BooleanArray::from(vec![false]));
    let data2 = Arc::new(Int32Array::from(vec![42]));
    let data3 = Arc::new(StringArray::from(vec!["foo"]));
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Boolean, false)),
            data1.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", DataType::Int32, false)),
            data2.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("z", DataType::Utf8, false)),
            data3.clone() as ArrayRef,
        ),
    ]);

    let res = df.with_column("new_col", Expr::Literal(ScalarValue::Struct(struct_array.into())))?;
    res.clone().show().await?;

    Ok(())
}

pub async fn df_cols_to_struct_example() -> Result<()> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    ).unwrap();

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch.clone()).unwrap();

    let cols = vec!["id", "name", "data"];
    let df_cols_to_struct = df.clone().select_columns(&cols)?;

    let batches = df_cols_to_struct.collect().await?;
    let mut pkeys = vec![];
    let mut names = vec![];
    let mut data_all = vec![];
    for batch in &batches {
        let xs = batch.columns();
        let pk = xs.first().unwrap().as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>();
        let name = xs.get(1).unwrap().as_any().downcast_ref::<StringArray>().unwrap().iter().collect::<Vec<_>>();
        let data = xs.get(2).unwrap().as_any().downcast_ref::<Int32Array>().unwrap().iter().collect::<Vec<_>>();
        pkeys.extend(pk);
        names.extend(name);
        data_all.extend(data);
    }

    let schema = Schema::new(vec![
        Field::new("id2", DataType::Int32, false),
        Field::new("metadata", DataType::Struct(Fields::from(vec![
                Field::new("name", DataType::Utf8, false), 
                Field::new("data", DataType::Int32, false),
            ])), false),
        ]
    );
    let ids = Int32Array::from(pkeys);
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, false)),
            Arc::new(StringArray::from(names)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("data", DataType::Int32, false)),
            Arc::new(Int32Array::from(data_all)) as ArrayRef,
        ),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(ids),
            Arc::new(struct_array),
        ],
    )?;
    let df_struct = ctx.read_batch(batch)?;

    let res = df
        .join(df_struct, JoinType::Inner, &["id"], &["id2"], None)?
        .select_columns(&["id", "foo", "metadata"])?;

    res.show().await?;

    Ok(())
}