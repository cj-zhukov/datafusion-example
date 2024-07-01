use crate::add_pk_to_df;

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use datafusion::{arrow::{array::{Int32Array, RecordBatch, StringArray}, datatypes::{DataType, Field, Schema}}, prelude::*};
use itertools::Itertools;
use serde_json::{Map, Value};
use tokio_stream::StreamExt;

/// Create json like string column by creating extra df and joining later, df must have primary key with int type.
/// Consider using df_cols_to_json from lib.rs because it's faster
pub async fn df_cols_to_json(ctx: SessionContext, df: DataFrame, cols: &[&str], pk: &str, new_col: Option<&str>, drop_pk: Option<bool>) -> Result<DataFrame> {
    let mut cols_new = cols.iter().map(|x| x.to_owned()).collect::<Vec<_>>();
    cols_new.push(pk);
    
    let df_cols_for_json = df.clone().select_columns(&cols_new)?;
    let mut stream = df_cols_for_json.clone().execute_stream().await?;
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch)?;
    }
    writer.finish()?;
    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;
    let mut res = HashMap::new();
    for mut json in json_rows {
        let pk = json.remove(pk).unwrap().to_string().parse::<i32>()?;
        res.extend(HashMap::from([(pk, json)]));
    }
    // println!("res:{:?}", res)
    let mut primary_keys = vec![];
    let mut data_all = vec![];
    for i in res.keys().sorted() {
        primary_keys.push(*i);
        let row = res[i].clone();
        // add Option type for json string like col
        let str_row = if !row.is_empty() {
            let str_row = serde_json::to_string(&row)?;
            Some(str_row)
        } else {
            None
        };
        data_all.push(str_row);
    }

    let mut right_cols = pk.to_string();
    right_cols.push_str("tojoin");
    let schema = Schema::new(vec![
        Field::new(right_cols.clone(), DataType::Int32, false),
        Field::new(new_col.unwrap_or("metadata"), DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(primary_keys)),
            Arc::new(StringArray::from(data_all)),
        ],
    )?;
    let df_to_json = ctx.read_batch(batch.clone())?;
    
    let res = df.join(df_to_json, JoinType::Inner, &[pk], &[&right_cols], None)?;
    
    let cols_new = match drop_pk {
        None => cols_new,
        Some(val) => match val {
            true => cols_new,
            false => cols_new.into_iter().filter(|x| !x.contains(pk)).collect::<Vec<_>>()
        }
    };

    let columns = res
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .filter(|x| !x.contains("tojoin"))
        .filter(|x| !cols_new.iter().any(|col| col.contains(x)))
        .collect::<Vec<_>>();
    // println!("{:?}", columns);

    let res = res.clone().select_columns(&columns)?;

    Ok(res)
}

/// Create json like string column by creating extra df and joining later.
/// Consider using df_cols_to_json from lib.rs because it's faster
pub async fn df_cols_to_json2(ctx: SessionContext, df: DataFrame, cols: &[&str], new_col: Option<&str>) -> Result<DataFrame> {
    let pk = "pk";
    let df = add_pk_to_df(ctx.clone(), df, pk).await?;
    let mut cols_new = cols.iter().map(|x| x.to_owned()).collect::<Vec<_>>();
    cols_new.push(pk);
    
    let df_cols_for_json = df.clone().select_columns(&cols_new)?;
    let mut stream = df_cols_for_json.clone().execute_stream().await?;
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch)?;
    }
    writer.finish()?;
    let json_data = writer.into_inner();
    let json_rows: Vec<Map<String, Value>> = serde_json::from_reader(json_data.as_slice())?;
    let mut res = HashMap::new();
    for mut json in json_rows {
        let pk = json.remove(pk).unwrap().to_string().parse::<i32>()?;
        res.extend(HashMap::from([(pk, json)]));
    }
    // println!("res:{:?}", res)
    let mut primary_keys = vec![];
    let mut data_all = vec![];
    for i in res.keys().sorted() {
        primary_keys.push(*i);
        let row = res[i].clone();
        // add Option type for json string like col
        let str_row = if !row.is_empty() {
            let str_row = serde_json::to_string(&row)?;
            Some(str_row)
        } else {
            None
        };
        data_all.push(str_row);
    }

    let mut right_cols = pk.to_string();
    right_cols.push_str("tojoin");
    let schema = Schema::new(vec![
        Field::new(right_cols.clone(), DataType::Int32, false),
        Field::new(new_col.unwrap_or("metadata"), DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(primary_keys)),
            Arc::new(StringArray::from(data_all)),
        ],
    )?;
    let df_to_json = ctx.read_batch(batch.clone())?;
    
    let res = df.join(df_to_json, JoinType::Inner, &[pk], &[&right_cols], None)?;
    
    let columns = res
        .schema()
        .fields()
        .iter()
        .map(|x| x.name().as_str())
        .filter(|x| !x.contains("tojoin"))
        .filter(|x| !cols_new.iter().any(|col| col.eq(x)))
        .collect::<Vec<_>>();

    let res = res.clone().select_columns(&columns)?;

    Ok(res)
}


#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;

    #[tokio::test]
    async fn test_cols_to_json() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("pkey", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("data", DataType::Int32, true),
        ]);
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                Arc::new(Int32Array::from(vec![42, 43, 44])),
            ],
        ).unwrap();
    
        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch).unwrap();
        let res = df_cols_to_json(ctx, df, &["name", "data"], "pkey", Some("metadata"), Some(true)).await.unwrap();

        assert_eq!(res.schema().fields().len(), 2);
        assert_eq!(res.clone().count().await.unwrap(), 3);
        
        let rows = res.sort(vec![col("id").sort(true, true)]).unwrap();
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
    }

    #[tokio::test]
    async fn test_cols_to_json2() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("data", DataType::Int32, true),
        ]);
        let batch = RecordBatch::try_new(
            schema.into(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                Arc::new(Int32Array::from(vec![42, 43, 44])),
            ],
        ).unwrap();
    
        let ctx = SessionContext::new();
        let df = ctx.read_batch(batch).unwrap();
        let res = df_cols_to_json2(ctx, df, &["name", "data"], Some("metadata")).await.unwrap();

        assert_eq!(res.schema().fields().len(), 2); // columns count
        assert_eq!(res.clone().count().await.unwrap(), 3); // rows count

        let rows = res.sort(vec![col("id").sort(true, true)]).unwrap();
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
    }
}