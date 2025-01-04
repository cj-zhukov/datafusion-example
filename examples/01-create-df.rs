use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, ListArray, RecordBatch, StringArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

#[tokio::main]
async fn main() -> Result<()> {
    create_df1().await?;
    create_df2().await?;
    create_df3().await?;
    create_df4().await?;
    create_df5().await?;
    create_df_struct1().await?;
    create_df_struct2().await?;
    create_df_list_arr().await?;
    
    Ok(())
}

pub async fn create_df1() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;
    let df = ctx.read_batch(batch)?;

    df.show().await?;

    Ok(())
}

pub async fn create_df2() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df = ctx.read_batch(batch)?;

    df.show().await?;

    Ok(())
}

pub async fn create_df3() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("pkey", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![None, None, Some("baz")])),
            Arc::new(Int32Array::from(vec![None, None, Some(44)])),
        ],
    )?;
    let df = ctx.read_batch(batch)?;

    df.show().await?;

    Ok(())
}

pub async fn create_df4() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("foo", DataType::Utf8, true),
        Field::new("bar", DataType::Utf8, true),
        Field::new("baz", DataType::Utf8, true),
    ]);
    
    let id: Int32Array = Int32Array::from_iter(0..10 as i32);
    let foo: StringArray = std::iter::repeat(Some("foo")).take(10).collect();
    let bar: StringArray = std::iter::repeat(Some("bar")).take(10).collect();
    let baz: StringArray = std::iter::repeat(Some("baz")).take(10).collect();

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id),
            Arc::new(foo),
            Arc::new(bar),
            Arc::new(baz),
        ],
    )?;

    let df = ctx.read_batch(batch)?;

    df.show().await?;

    Ok(())
}

pub async fn create_df5() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("pkey", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Int32, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from_iter(0..3_i32)),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let df = ctx.read_batch(batch)?;

    df.show().await?;

    Ok(())
}

pub async fn create_df_struct1() -> Result<()> {
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
        Arc::new(schema),
        vec![
            Arc::new(ids),
            Arc::new(struct_array),
        ],
    )?;

    let ctx = SessionContext::new();
    let res = ctx.read_batch(batch)?;

    res.show().await?;

    Ok(())
}

pub async fn create_df_struct2() -> Result<()> {
    let ctx = SessionContext::new();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
        ],
    )?;
    let df = ctx.read_batch(batch)?;

    let data1 = Arc::new(BooleanArray::from(vec![false]));
    let data2 = Arc::new(Int32Array::from(vec![42]));
    let data3 = Arc::new(StringArray::from(vec!["foo"]));
    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Boolean, false)),
            data1 as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", DataType::Int32, false)),
            data2 as ArrayRef,
        ),
        (
            Arc::new(Field::new("z", DataType::Utf8, false)),
            data3 as ArrayRef,
        ),
    ]);

    let res = df.with_column("new_col", Expr::Literal(ScalarValue::Struct(struct_array.into())))?;
    
    res.show().await?;

    Ok(())
}

pub async fn create_df_list_arr() -> Result<()> {
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
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
            Arc::new(list_array),
        ],
    )?;

    let ctx = SessionContext::new();
    let res = ctx.read_batch(batch)?;

    res.show().await?;

    Ok(())
}