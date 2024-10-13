use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::prelude::*;
use datafusion_example::utils::scalarvalue::ScalarValueNew;
use itertools::Itertools;
use serde_json::{Map, Value};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    demo_struct().await?;
    record_batches_to_json_rows()?;
    df_cols_to_json_example().await?;
    df_cols_to_struct_example().await?;
    scalar_new_example().await?;

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
    // println!("{:?}", json_rows);

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
    // df.clone().show().await?;

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
    // println!("res:{:?}", res);

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

    let res = df.join(df_to_json, JoinType::Inner, &["id"], &["id"], None)?;
    res.show().await?;

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
        Field::new("item", DataType::Struct(Fields::from(vec![
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
        .select_columns(&["id", "name", "item"])?;
    
    res.show().await?;
    
    Ok(())
}

pub async fn scalar_new_example() -> Result<()> {
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

    let mut stream = df.execute_stream().await?; 
    let mut columns: HashMap<usize, Vec<ScalarValueNew>> = HashMap::new();
    while let Some(batch) = stream.next().await.transpose()? {
        for i in 0..batch.num_columns() {
            let arr = batch.column(i);
            for idx in 0..arr.len() {
                let value = ScalarValueNew::try_from_array(arr, idx)?;
                let data = vec![value];
                match columns.get_mut(&i) {
                    Some(val) => {
                        val.extend(data);
                    },
                    None => {
                        columns.insert(i, data);
                    }
                }
            }

        }        
    }

    let mut id_all = vec![];
    let x = columns[&0].clone();
    for column in x {
        if let ScalarValueNew::Int32(res) = column {
            id_all.push(res);
        }
    }
    let mut name_all = vec![];
    let x = columns[&1].clone();
    for column in x {
        if let ScalarValueNew::Utf8(res) = column {
            name_all.push(res);
        }
    }
    let mut data_all = vec![];
    let x = columns[&2].clone();
    for column in x {
        if let ScalarValueNew::Int32(res) = column {
            data_all.push(res);
        }
    }

    let id: ArrayRef = Arc::new(Int32Array::from(id_all));
    let name: ArrayRef = Arc::new(StringArray::from(name_all));
    let data: ArrayRef = Arc::new(Int32Array::from(data_all));
    let record_batch = RecordBatch::try_from_iter_with_nullable(vec![
        ("id", id, true),
        ("name", name, true),
        ("data", data, true),
    ])?;
    let df = ctx.read_batch(record_batch)?;
    df.show().await?;

    Ok(())
}