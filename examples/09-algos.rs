use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{
    ArrayRef, Float64Array, Float64Builder, Int32Array, RecordBatch, StringArray, StringBuilder,
    StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::prelude::*;
use datafusion_example::utils::dataframe::df_to_table;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<()> {
    round_robin_example().await?;
    random_example().await?;
    least_values_example().await?;
    one_billion_row_challenge().await?;
    Ok(())
}

/// Round-Robin Selection of Workers
pub async fn round_robin_example() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Float64, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Float64Array::from(vec![1.0, 10.0, 100.0])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;
    let table_name = "t";
    df_to_table(&ctx, df, table_name).await?;

    let mut cur_worker = 1;
    while cur_worker <= 5 {
        let round_robin_sql =
            format!("(({cur_worker} - 1) % (select count(*) from {table_name})) + 1");
        let df = ctx
            .sql(&format!(
                "select * from {table_name} where id = {round_robin_sql}"
            ))
            .await?;
        df.show().await?;
        cur_worker += 1;
    }

    Ok(())
}

pub async fn random_example() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Float64, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Float64Array::from(vec![1.0, 10.0, 100.0])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;
    let table_name = "t";
    df_to_table(&ctx, df, table_name).await?;

    let sql = format!(
        "select *
                from {table_name} 
                where 1 = 1 
                order by random()
                limit 1"
    );

    for _ in 0..5 {
        let df = ctx.sql(&sql).await?;
        df.show().await?;
    }

    Ok(())
}

pub async fn least_values_example() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Float64, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Float64Array::from(vec![1.0, 10.0, 100.0])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;
    let table_name = "t";
    let col_val = "id";
    df_to_table(&ctx, df, table_name).await?;

    let sql = format!(
        "select *
                from {table_name} 
                where 1 = 1 
                and {col_val} = (select min({col_val}) from {table_name})"
    );

    let df = ctx.sql(&sql).await?;
    df.show().await?;

    Ok(())
}

pub async fn one_billion_row_challenge() -> Result<()> {
    let csv_data = r#"Tokyo;35.6897
Jakarta;-6.1750
Delhi;28.6100
Guangzhou;23.1300
Mumbai;19.0761
Manila;14.5958
Shanghai;31.1667
São Paulo;-23.5500
Seoul;37.5600
Mexico City;19.4333
Cairo;30.0444
New York;40.6943
Dhaka;23.7639
Beijing;39.9040
Kolkāta;22.5675
Bangkok;13.7525
Shenzhen;22.5350
Moscow;55.7558
Buenos Aires;-34.5997
Lagos;6.4550
Istanbul;41.0136
Karachi;24.8600
Bangalore;12.9789
Ho Chi Minh City;10.7756
Ōsaka;34.6939
Chengdu;30.6600
Tehran;35.6892
Kinshasa;-4.3250
Rio de Janeiro;-22.9111
Chennai;13.0825
Xi’an;34.2667
Lahore;31.5497
Chongqing;29.5500
Los Angeles;34.1141
Baoding;38.8671
London;51.5072
Paris;48.8567
Linyi;35.1041
Dongguan;23.0475
Hyderābād;17.3850
Tianjin;39.1467
Lima;-12.0600
Wuhan;30.5872
Nanyang;32.9987
Hangzhou;30.2500
Foshan;23.0292
Nagoya;35.1833
Taipei;25.0375
Tongshan;34.2610
Luanda;-8.8383
Zhoukou;33.6250
Ganzhou;25.8292
Kuala Lumpur;3.1478
Heze;35.2333
Quanzhou;24.9139
Chicago;41.8375
Nanjing;32.0608
Jining;35.4000
Hanoi;21.0283
Pune;18.5203
Fuyang;32.8986
Ahmedabad;23.0300
Johannesburg;-26.2044
Bogotá;4.7111
Dar es Salaam;-6.8161
Shenyang;41.8025
Khartoum;15.5006
Shangqiu;34.4259
Cangzhou;38.3037
Hong Kong;22.3000
Shaoyang;27.2418
Zhanjiang;21.1967
Yancheng;33.3936
Hengyang;26.8968
Riyadh;24.6333
Zhumadian;32.9773
Santiago;-33.4372
Xingtai;37.0659
Chattogram;22.3350
Bijie;27.3019
Shangrao;28.4419
Zunyi;27.7050
Sūrat;21.1702
Surabaya;-7.2458
Huanggang;30.4500
Maoming;21.6618
Nanchong;30.7991
Xinyang;32.1264
Madrid;40.4169
Baghdad;33.3153
Qujing;25.5102
Jieyang;23.5533
Singapore;1.3000
Prayagraj;25.4358
Liaocheng;36.4500
Dalian;38.9000
Yulin;22.6293
Changde;29.0397
Qingdao;36.1167
Douala;4.0500"#;
    let dir = tempdir()?;
    let file_path = dir.path().join("weather_stations.csv");
    {
        let mut file = File::create(&file_path)?;
        file.write_all(csv_data.as_bytes())?;
    }
    let file_path = file_path.to_str().unwrap();
    let ctx = SessionContext::new();
    let options = CsvReadOptions::new().has_header(false);
    ctx.register_csv("weather_stations", file_path, options).await?;

    let return_type = DataType::Struct(Fields::from(vec![
        Arc::new(Field::new("city", DataType::Utf8, true)),
        Arc::new(Field::new("temperature", DataType::Float64, true)),
    ]));

    // ------- UDF: extract city and temp (string and float64) -------
    let fun = Arc::new(|args: &[ColumnarValue]| {
        let input = match &args[0] {
            ColumnarValue::Array(array) => array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution("Invalid input array".to_string())
                })?,
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Expected array as input".to_string(),
                ));
            }
        };

        let mut city_builder = StringBuilder::new();
        let mut temp_builder = Float64Builder::new();
        for maybe_str in input.iter() {
            if let Some(s) = maybe_str {
                let mut parts = s.split(';');
                // ---- city ----
                if let Some(city) = parts.next() {
                    city_builder.append_value(city);
                } else {
                    city_builder.append_null();
                }

                // ---- temperature ----
                if let Some(temp_str) = parts.next() {
                    if let Ok(v) = temp_str.parse::<f64>() {
                        temp_builder.append_value(v);
                    } else {
                        temp_builder.append_null();
                    }
                } else {
                    temp_builder.append_null();
                }
            } else {
                city_builder.append_null();
                temp_builder.append_null();
            }
        }

        let city_array = Arc::new(city_builder.finish()) as ArrayRef;
        let temp_array = Arc::new(temp_builder.finish()) as ArrayRef;
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("city", DataType::Utf8, true)),
                city_array,
            ),
            (
                Arc::new(Field::new("temperature", DataType::Float64, true)),
                temp_array,
            ),
        ]);

        Ok(ColumnarValue::Array(Arc::new(struct_array)))
    });

    let udf = create_udf(
        "split_row",
        vec![DataType::Utf8],
        return_type,
        Volatility::Immutable,
        fun,
    );
    ctx.register_udf(udf);

    let res = ctx
        .sql(
            "SELECT city,
                    min(temperature) AS min_temperature,
                    max(temperature) AS max_temperature,
                    median(temperature) AS median_temperature
             FROM (
                SELECT 
                    get_field(split_row(column_1), 'city') AS city,
                    get_field(split_row(column_1), 'temperature') AS temperature
                FROM weather_stations
             )
             GROUP BY city
             ORDER BY city
             LIMIT 100",
        )
        .await?;
    res.show().await?;

    Ok(())
}
