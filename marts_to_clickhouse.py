import os
from decimal import Decimal

import pandas as pd
import psycopg2
from clickhouse_driver import Client
from pyspark.sql import SparkSession, functions as F

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'bigdata_lab')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '9000')
)
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'marts')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse')


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName('bigdata-lab-marts-to-clickhouse')
        .master('local[*]')
        .config('spark.sql.shuffle.partitions', '4')
        .getOrCreate()
    )


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def get_clickhouse_client():
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def fetch_star_tables() -> dict:
    queries = {
        'fact_sales': 'SELECT * FROM fact_sales',
        'dim_customers': 'SELECT * FROM dim_customers',
        'dim_sellers': 'SELECT * FROM dim_sellers',
        'dim_products': 'SELECT * FROM dim_products',
        'dim_stores': 'SELECT * FROM dim_stores',
        'dim_suppliers': 'SELECT * FROM dim_suppliers',
        'dim_dates': 'SELECT * FROM dim_dates',
    }
    conn = get_postgres_connection()
    try:
        return {name: pd.read_sql(query, conn) for name, query in queries.items()}
    finally:
        conn.close()


def prepare_dataframe_for_spark(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.copy()
    for col in pdf.columns:
        if pd.api.types.is_object_dtype(pdf[col]):
            pdf[col] = pdf[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
        elif pd.api.types.is_numeric_dtype(pdf[col]):
            pdf[col] = pdf[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
    return pdf


def build_enriched_sales_df(spark: SparkSession, tables: dict):
    fact_sales = spark.createDataFrame(prepare_dataframe_for_spark(tables['fact_sales']))
    dim_customers = spark.createDataFrame(prepare_dataframe_for_spark(tables['dim_customers']))
    dim_sellers = spark.createDataFrame(prepare_dataframe_for_spark(tables['dim_sellers']))
    dim_products = spark.createDataFrame(prepare_dataframe_for_spark(tables['dim_products']))
    dim_stores = spark.createDataFrame(prepare_dataframe_for_spark(tables['dim_stores']))
    dim_suppliers = spark.createDataFrame(prepare_dataframe_for_spark(tables['dim_suppliers']))
    dim_dates = spark.createDataFrame(prepare_dataframe_for_spark(tables['dim_dates']))

    enriched_df = (
        fact_sales.alias('f')
        .join(dim_customers.alias('c'), on='customer_key', how='left')
        .join(dim_sellers.alias('s'), on='seller_key', how='left')
        .join(dim_products.alias('p'), on='product_key', how='left')
        .join(dim_stores.alias('st'), on='store_key', how='left')
        .join(dim_suppliers.alias('sup'), on='supplier_key', how='left')
        .join(dim_dates.alias('d'), on='date_key', how='left')
        .withColumn('customer_name', F.concat_ws(' ', F.col('customer_first_name'), F.col('customer_last_name')))
        .withColumn('seller_name', F.concat_ws(' ', F.col('seller_first_name'), F.col('seller_last_name')))
        .withColumn('sale_total_price', F.round(F.col('sale_total_price').cast('double'), 2))
        .withColumn('product_price', F.round(F.col('product_price').cast('double'), 2))
        .withColumn('product_rating', F.round(F.col('product_rating').cast('double'), 2))
    )
    return enriched_df


def mart_sales_products(enriched_df):
    return (
        enriched_df
        .groupBy('product_key', 'product_name', 'product_category')
        .agg(
            F.countDistinct('source_sale_id').alias('total_orders'),
            F.sum('sale_quantity').alias('total_units_sold'),
            F.round(F.sum('sale_total_price'), 2).alias('total_revenue'),
            F.round(F.avg('product_rating'), 2).alias('avg_rating'),
            F.max('product_reviews').alias('total_reviews')
        )
        .orderBy(F.desc('total_revenue'), F.desc('total_units_sold'))
        .limit(10)
    )


def mart_sales_customers(enriched_df):
    return (
        enriched_df
        .groupBy('customer_key', 'customer_name', 'customer_country')
        .agg(
            F.countDistinct('source_sale_id').alias('total_orders'),
            F.round(F.sum('sale_total_price'), 2).alias('total_revenue'),
            F.round(F.avg('sale_total_price'), 2).alias('avg_check')
        )
        .orderBy(F.desc('total_revenue'), F.desc('total_orders'))
        .limit(10)
    )


def mart_sales_time(enriched_df):
    return (
        enriched_df
        .groupBy('year_num', 'quarter_num', 'month_num', 'month_name')
        .agg(
            F.countDistinct('source_sale_id').alias('total_orders'),
            F.sum('sale_quantity').alias('total_units_sold'),
            F.round(F.sum('sale_total_price'), 2).alias('total_revenue'),
            F.round(F.avg('sale_total_price'), 2).alias('avg_order_value')
        )
        .orderBy('year_num', 'month_num')
    )


def mart_sales_stores(enriched_df):
    return (
        enriched_df
        .groupBy('store_key', 'store_name', 'store_city', 'store_country')
        .agg(
            F.countDistinct('source_sale_id').alias('total_orders'),
            F.round(F.sum('sale_total_price'), 2).alias('total_revenue'),
            F.round(F.avg('sale_total_price'), 2).alias('avg_check')
        )
        .orderBy(F.desc('total_revenue'), F.desc('total_orders'))
        .limit(5)
    )


def mart_sales_suppliers(enriched_df):
    return (
        enriched_df
        .groupBy('supplier_key', 'supplier_name', 'supplier_country')
        .agg(
            F.countDistinct('source_sale_id').alias('total_orders'),
            F.round(F.sum('sale_total_price'), 2).alias('total_revenue'),
            F.round(F.avg('product_price'), 2).alias('avg_product_price'),
            F.sum('sale_quantity').alias('total_units_sold')
        )
        .orderBy(F.desc('total_revenue'), F.desc('total_orders'))
        .limit(5)
    )


def mart_product_quality(enriched_df):
    return (
        enriched_df
        .groupBy('product_key', 'product_name')
        .agg(
            F.round(F.avg('product_rating'), 2).alias('product_rating'),
            F.max('product_reviews').alias('product_reviews'),
            F.sum('sale_quantity').alias('total_units_sold'),
            F.countDistinct('source_sale_id').alias('total_orders'),
            F.round(F.sum('sale_total_price'), 2).alias('total_revenue')
        )
        .withColumn(
            'revenue_per_review',
            F.when(
                F.col('product_reviews') > 0,
                F.round(F.col('total_revenue') / F.col('product_reviews'), 2)
            ).otherwise(F.lit(None).cast('double'))
        )
        .orderBy(F.desc('total_revenue'), F.desc('product_rating'))
    )


def pandas_rows(df, columns):
    pdf = df.select(*columns).toPandas().where(pd.notnull, None)
    return [tuple(row) for row in pdf.itertuples(index=False, name=None)]


def execute_clickhouse_ddl(client) -> None:
    ddl_statements = [
        f'CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}',

        f'''
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.mart_sales_products (
            product_key UInt64,
            product_name Nullable(String),
            product_category Nullable(String),
            total_orders UInt64,
            total_units_sold Int64,
            total_revenue Float64,
            avg_rating Nullable(Float64),
            total_reviews Nullable(Int64)
        ) ENGINE = MergeTree()
        ORDER BY (total_revenue, product_key)
        ''',

        f'''
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.mart_sales_customers (
            customer_key UInt64,
            customer_name Nullable(String),
            customer_country Nullable(String),
            total_orders UInt64,
            total_revenue Float64,
            avg_check Float64
        ) ENGINE = MergeTree()
        ORDER BY (total_revenue, customer_key)
        ''',

        f'''
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.mart_sales_time (
            year_num Int32,
            quarter_num Int32,
            month_num Int32,
            month_name Nullable(String),
            total_orders UInt64,
            total_units_sold Int64,
            total_revenue Float64,
            avg_order_value Float64
        ) ENGINE = MergeTree()
        ORDER BY (year_num, month_num)
        ''',

        f'''
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.mart_sales_stores (
            store_key UInt64,
            store_name Nullable(String),
            store_city Nullable(String),
            store_country Nullable(String),
            total_orders UInt64,
            total_revenue Float64,
            avg_check Float64
        ) ENGINE = MergeTree()
        ORDER BY (total_revenue, store_key)
        ''',

        f'''
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.mart_sales_suppliers (
            supplier_key UInt64,
            supplier_name Nullable(String),
            supplier_country Nullable(String),
            total_orders UInt64,
            total_revenue Float64,
            avg_product_price Nullable(Float64),
            total_units_sold Int64
        ) ENGINE = MergeTree()
        ORDER BY (total_revenue, supplier_key)
        ''',

        f'''
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.mart_product_quality (
            product_key UInt64,
            product_name Nullable(String),
            product_rating Nullable(Float64),
            product_reviews Nullable(Int64),
            total_units_sold Int64,
            total_orders UInt64,
            total_revenue Float64,
            revenue_per_review Nullable(Float64)
        ) ENGINE = MergeTree()
        ORDER BY (total_revenue, product_key)
        '''
    ]

    for sql in ddl_statements:
        client.execute(sql)


def truncate_clickhouse_tables(client) -> None:
    tables = [
        'mart_sales_products',
        'mart_sales_customers',
        'mart_sales_time',
        'mart_sales_stores',
        'mart_sales_suppliers',
        'mart_product_quality',
    ]
    for table in tables:
        client.execute(f'TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DB}.{table}')


def load_marts_to_clickhouse(client, marts: dict) -> None:
    for table_name, (df, columns) in marts.items():
        rows = pandas_rows(df, columns)
        if rows:
            client.execute(
                f'INSERT INTO {CLICKHOUSE_DB}.{table_name} ({", ".join(columns)}) VALUES',
                rows
            )
        print(f'{table_name}: загружено строк {len(rows)}')


def print_clickhouse_counts(client) -> None:
    for table in [
        'mart_sales_products',
        'mart_sales_customers',
        'mart_sales_time',
        'mart_sales_stores',
        'mart_sales_suppliers',
        'mart_product_quality',
    ]:
        cnt = client.execute(f'SELECT count() FROM {CLICKHOUSE_DB}.{table}')[0][0]
        print(f'{table}: {cnt}')


def main() -> None:
    print('=== ETL: PostgreSQL star schema -> ClickHouse marts ===')
    spark = build_spark()
    try:
        tables = fetch_star_tables()
        enriched_df = build_enriched_sales_df(spark, tables)

        marts = {
            'mart_sales_products': (
                mart_sales_products(enriched_df),
                ['product_key', 'product_name', 'product_category', 'total_orders', 'total_units_sold', 'total_revenue', 'avg_rating', 'total_reviews']
            ),
            'mart_sales_customers': (
                mart_sales_customers(enriched_df),
                ['customer_key', 'customer_name', 'customer_country', 'total_orders', 'total_revenue', 'avg_check']
            ),
            'mart_sales_time': (
                mart_sales_time(enriched_df),
                ['year_num', 'quarter_num', 'month_num', 'month_name', 'total_orders', 'total_units_sold', 'total_revenue', 'avg_order_value']
            ),
            'mart_sales_stores': (
                mart_sales_stores(enriched_df),
                ['store_key', 'store_name', 'store_city', 'store_country', 'total_orders', 'total_revenue', 'avg_check']
            ),
            'mart_sales_suppliers': (
                mart_sales_suppliers(enriched_df),
                ['supplier_key', 'supplier_name', 'supplier_country', 'total_orders', 'total_revenue', 'avg_product_price', 'total_units_sold']
            ),
            'mart_product_quality': (
                mart_product_quality(enriched_df),
                ['product_key', 'product_name', 'product_rating', 'product_reviews', 'total_units_sold', 'total_orders', 'total_revenue', 'revenue_per_review']
            ),
        }

        client = get_clickhouse_client()
        try:
            execute_clickhouse_ddl(client)
            truncate_clickhouse_tables(client)
            load_marts_to_clickhouse(client, marts)
            print_clickhouse_counts(client)
        finally:
            client.disconnect()
    finally:
        spark.stop()


if __name__ == '__main__':
    main()