import os

import clickhouse_connect
from pyspark.sql import SparkSession, functions as F


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "bigdata_lab")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_JDBC_PORT = int(os.getenv("CLICKHOUSE_JDBC_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "marts")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")

SPARK_JARS = os.getenv(
    "SPARK_JARS",
    "/opt/bitnami/spark/jars/postgresql-42.7.10.jar,"
    "/opt/bitnami/spark/jars/clickhouse-jdbc-0.7.1.jar",
)

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_JDBC_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_JDBC_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_JDBC_PORT}/{CLICKHOUSE_DB}"
CLICKHOUSE_JDBC_PROPS = {
    "user": CLICKHOUSE_USER,
    "password": CLICKHOUSE_PASSWORD,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("bigdata-lab-marts-to-clickhouse")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars", SPARK_JARS)
        .config("spark.driver.extraClassPath", SPARK_JARS.replace(",", ":"))
        .config("spark.executor.extraClassPath", SPARK_JARS.replace(",", ":"))
    )
    return builder.getOrCreate()


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def prepare_clickhouse() -> None:
    client = get_clickhouse_client()
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

        ddl_statements = [
            f"""
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
            ORDER BY (product_key)
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.mart_sales_customers (
                customer_key UInt64,
                customer_name Nullable(String),
                customer_country Nullable(String),
                total_orders UInt64,
                total_revenue Float64,
                avg_check Float64
            ) ENGINE = MergeTree()
            ORDER BY (customer_key)
            """,
            f"""
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
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.mart_sales_stores (
                store_key UInt64,
                store_name Nullable(String),
                store_city Nullable(String),
                store_country Nullable(String),
                total_orders UInt64,
                total_revenue Float64,
                avg_check Float64
            ) ENGINE = MergeTree()
            ORDER BY (store_key)
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.mart_sales_suppliers (
                supplier_key UInt64,
                supplier_name Nullable(String),
                supplier_country Nullable(String),
                total_orders UInt64,
                total_revenue Float64,
                avg_product_price Nullable(Float64),
                total_units_sold Int64
            ) ENGINE = MergeTree()
            ORDER BY (supplier_key)
            """,
            f"""
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
            ORDER BY (product_key)
            """,
        ]

        for ddl in ddl_statements:
            client.command(ddl)

        for table in [
            "mart_sales_products",
            "mart_sales_customers",
            "mart_sales_time",
            "mart_sales_stores",
            "mart_sales_suppliers",
            "mart_product_quality",
        ]:
            client.command(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DB}.{table}")
    finally:
        client.close()


def read_pg_table(spark: SparkSession, table_name: str):
    return spark.read.jdbc(
        url=POSTGRES_JDBC_URL,
        table=table_name,
        properties=POSTGRES_JDBC_PROPS,
    )


def build_enriched_sales_df(spark: SparkSession):
    fact_sales = read_pg_table(spark, "public.fact_sales")
    dim_customers = read_pg_table(spark, "public.dim_customers")
    dim_sellers = read_pg_table(spark, "public.dim_sellers")
    dim_products = read_pg_table(spark, "public.dim_products")
    dim_stores = read_pg_table(spark, "public.dim_stores")
    dim_suppliers = read_pg_table(spark, "public.dim_suppliers")
    dim_dates = read_pg_table(spark, "public.dim_dates")

    enriched_df = (
        fact_sales.alias("f")
        .join(dim_customers.alias("c"), on="customer_key", how="left")
        .join(dim_sellers.alias("s"), on="seller_key", how="left")
        .join(dim_products.alias("p"), on="product_key", how="left")
        .join(dim_stores.alias("st"), on="store_key", how="left")
        .join(dim_suppliers.alias("sup"), on="supplier_key", how="left")
        .join(dim_dates.alias("d"), on="date_key", how="left")
        .withColumn("customer_name", F.concat_ws(" ", F.col("customer_first_name"), F.col("customer_last_name")))
        .withColumn("seller_name", F.concat_ws(" ", F.col("seller_first_name"), F.col("seller_last_name")))
        .withColumn("sale_total_price", F.round(F.col("sale_total_price").cast("double"), 2))
        .withColumn("product_price", F.round(F.col("product_price").cast("double"), 2))
        .withColumn("product_rating", F.round(F.col("product_rating").cast("double"), 2))
        .withColumn("sale_quantity", F.col("sale_quantity").cast("long"))
        .withColumn("product_reviews", F.col("product_reviews").cast("long"))
        .withColumn("year_num", F.col("year_num").cast("int"))
        .withColumn("quarter_num", F.col("quarter_num").cast("int"))
        .withColumn("month_num", F.col("month_num").cast("int"))
    )

    return enriched_df


def mart_sales_products(enriched_df):
    return (
        enriched_df
        .groupBy("product_key", "product_name", "product_category")
        .agg(
            F.countDistinct("source_sale_id").alias("total_orders"),
            F.sum("sale_quantity").alias("total_units_sold"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("product_rating"), 2).alias("avg_rating"),
            F.max("product_reviews").alias("total_reviews"),
        )
        .orderBy(F.desc("total_revenue"), F.desc("total_units_sold"))
        .limit(10)
    )


def mart_sales_customers(enriched_df):
    return (
        enriched_df
        .groupBy("customer_key", "customer_name", "customer_country")
        .agg(
            F.countDistinct("source_sale_id").alias("total_orders"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
        .orderBy(F.desc("total_revenue"), F.desc("total_orders"))
        .limit(10)
    )


def mart_sales_time(enriched_df):
    return (
        enriched_df
        .groupBy("year_num", "quarter_num", "month_num", "month_name")
        .agg(
            F.countDistinct("source_sale_id").alias("total_orders"),
            F.sum("sale_quantity").alias("total_units_sold"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_order_value"),
        )
        .orderBy("year_num", "month_num")
    )


def mart_sales_stores(enriched_df):
    return (
        enriched_df
        .groupBy("store_key", "store_name", "store_city", "store_country")
        .agg(
            F.countDistinct("source_sale_id").alias("total_orders"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("sale_total_price"), 2).alias("avg_check"),
        )
        .orderBy(F.desc("total_revenue"), F.desc("total_orders"))
        .limit(5)
    )


def mart_sales_suppliers(enriched_df):
    return (
        enriched_df
        .groupBy("supplier_key", "supplier_name", "supplier_country")
        .agg(
            F.countDistinct("source_sale_id").alias("total_orders"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
            F.round(F.avg("product_price"), 2).alias("avg_product_price"),
            F.sum("sale_quantity").alias("total_units_sold"),
        )
        .orderBy(F.desc("total_revenue"), F.desc("total_orders"))
        .limit(5)
    )


def mart_product_quality(enriched_df):
    return (
        enriched_df
        .groupBy("product_key", "product_name")
        .agg(
            F.round(F.avg("product_rating"), 2).alias("product_rating"),
            F.max("product_reviews").alias("product_reviews"),
            F.sum("sale_quantity").alias("total_units_sold"),
            F.countDistinct("source_sale_id").alias("total_orders"),
            F.round(F.sum("sale_total_price"), 2).alias("total_revenue"),
        )
        .withColumn(
            "revenue_per_review",
            F.when(
                F.col("product_reviews") > 0,
                F.round(F.col("total_revenue") / F.col("product_reviews"), 2),
            ).otherwise(F.lit(None).cast("double"))
        )
        .orderBy(F.desc("total_revenue"), F.desc("product_rating"))
    )


def write_clickhouse_table(df, table_name: str) -> None:
    (
        df.write
        .mode("append")
        .jdbc(
            url=CLICKHOUSE_JDBC_URL,
            table=table_name,
            properties=CLICKHOUSE_JDBC_PROPS,
        )
    )


def print_clickhouse_counts() -> None:
    client = get_clickhouse_client()
    try:
        for table in [
            "mart_sales_products",
            "mart_sales_customers",
            "mart_sales_time",
            "mart_sales_stores",
            "mart_sales_suppliers",
            "mart_product_quality",
        ]:
            cnt = client.query(f"SELECT count() AS cnt FROM {CLICKHOUSE_DB}.{table}").first_row[0]
            print(f"{table}: {cnt}")
    finally:
        client.close()


def main() -> None:
    print("=== ETL: PostgreSQL star schema -> ClickHouse marts (через Spark JDBC) ===")

    prepare_clickhouse()

    spark = build_spark()
    try:
        enriched_df = build_enriched_sales_df(spark)

        marts = {
            "mart_sales_products": mart_sales_products(enriched_df).select(
                "product_key",
                "product_name",
                "product_category",
                "total_orders",
                "total_units_sold",
                "total_revenue",
                "avg_rating",
                "total_reviews",
            ),
            "mart_sales_customers": mart_sales_customers(enriched_df).select(
                "customer_key",
                "customer_name",
                "customer_country",
                "total_orders",
                "total_revenue",
                "avg_check",
            ),
            "mart_sales_time": mart_sales_time(enriched_df).select(
                "year_num",
                "quarter_num",
                "month_num",
                "month_name",
                "total_orders",
                "total_units_sold",
                "total_revenue",
                "avg_order_value",
            ),
            "mart_sales_stores": mart_sales_stores(enriched_df).select(
                "store_key",
                "store_name",
                "store_city",
                "store_country",
                "total_orders",
                "total_revenue",
                "avg_check",
            ),
            "mart_sales_suppliers": mart_sales_suppliers(enriched_df).select(
                "supplier_key",
                "supplier_name",
                "supplier_country",
                "total_orders",
                "total_revenue",
                "avg_product_price",
                "total_units_sold",
            ),
            "mart_product_quality": mart_product_quality(enriched_df).select(
                "product_key",
                "product_name",
                "product_rating",
                "product_reviews",
                "total_units_sold",
                "total_orders",
                "total_revenue",
                "revenue_per_review",
            ),
        }

        for table_name, df in marts.items():
            write_clickhouse_table(df, table_name)
            print(f"{table_name}: загружено строк {df.count()}")

        print_clickhouse_counts()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()