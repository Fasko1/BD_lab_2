# Big Data Spark — Лабораторная работа №2
## ETL-пайплайн на Apache Spark
М80-303Б-23 Кунавин Кирилл Витальевич
---

## 📌 Описание проекта

В рамках лабораторной работы реализован ETL-пайплайн с использованием Apache Spark.

Пайплайн выполняет следующие этапы:
1. Загрузка исходных данных из CSV-файлов в PostgreSQL (таблица `mock_data`)
2. Построение модели данных "звезда" (fact + dimension tables) в PostgreSQL
3. Формирование аналитических витрин (data marts) с помощью Spark
4. Загрузка витрин в ClickHouse (OLAP БД)

---
## 📌 Особенности данных

Исходные данные (mock_data) являются синтетическими и содержат большое количество уникальных комбинаций значений.

В результате:
- некоторые измерения (например, dim_stores и dim_suppliers) имеют размер, близкий к фактовой таблице
- это связано с высокой уникальностью данных, а не с ошибкой ETL-процесса

В реальных системах такие измерения обычно имеют значительно меньший размер за счёт повторяющихся значений и дополнительной нормализации.
## 🧱 Используемые технологии

- Apache Spark 3.5.1
- PostgreSQL 16
- ClickHouse
- Docker / Docker Compose
- Python (PySpark, pandas, psycopg2)

---

## 📁 Структура проекта

```bash
.
├── data/                      # CSV файлы (10 файлов по 1000 строк)
├── sql/
│   ├── init_postgres.sql      # создание таблицы mock_data
│   └── create_star_schema.sql # создание схемы звезда
├── etl_to_star.py             # ETL: CSV → PostgreSQL (звезда)
├── marts_to_clickhouse.py     # ETL: звезда → витрины (ClickHouse)
├── docker-compose.yml
├── requirements.txt
└── README_run.md
```

---

## 🚀 Запуск проекта

### 1. Запуск контейнеров

```bash
docker compose up -d
```

---

### 2. Запуск ETL в звезду (PostgreSQL)

```bash
docker exec -it bd_spark /opt/spark/bin/spark-submit etl_to_star.py
```

После выполнения:
- таблица `mock_data` содержит 10000 строк
- создаются таблицы:
  - fact_sales
  - dim_customers
  - dim_products
  - dim_sellers
  - dim_stores
  - dim_suppliers
  - dim_dates

---

### 3. Построение витрин в ClickHouse

```bash
docker exec -it bd_spark /opt/spark/bin/spark-submit marts_to_clickhouse.py
```

После выполнения создаются 6 витрин:

- mart_sales_products
- mart_sales_customers
- mart_sales_time
- mart_sales_stores
- mart_sales_suppliers
- mart_product_quality

---

## 📊 Описание витрин

### 1. Витрина продаж по продуктам
- топ-10 продуктов по выручке
- количество продаж
- средний рейтинг
- количество отзывов

### 2. Витрина продаж по клиентам
- топ-10 клиентов по сумме покупок
- средний чек
- распределение по странам

### 3. Витрина продаж по времени
- месячные и годовые тренды
- средний чек по месяцам

### 4. Витрина продаж по магазинам
- топ-5 магазинов по выручке
- средний чек
- география продаж

### 5. Витрина продаж по поставщикам
- топ-5 поставщиков
- средняя цена товаров
- распределение по странам

### 6. Витрина качества продукции
- рейтинг товаров
- количество отзывов
- связь рейтинга и продаж

---

## 🔍 Проверка данных

### PostgreSQL

```sql
SELECT COUNT(*) FROM mock_data;
SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM dim_products;
SELECT COUNT(*) FROM dim_stores;
SELECT COUNT(*) FROM dim_suppliers;
```

---

### ClickHouse

```sql
SELECT count() FROM marts.mart_sales_products;
SELECT count() FROM marts.mart_sales_customers;
SELECT count() FROM marts.mart_sales_time;
SELECT count() FROM marts.mart_sales_stores;
SELECT count() FROM marts.mart_sales_suppliers;
SELECT count() FROM marts.mart_product_quality;
```

---

## ⚠️ Примечания

- Данные автоматически загружаются в PostgreSQL через Spark (ETL-пайплайн)
- В качестве OLAP-хранилища используется ClickHouse
- Дополнительные NoSQL БД (Cassandra, MongoDB и др.) не реализованы (опционально)

---

## ✅ Результат

- Реализован полный ETL-пайплайн
- Построена модель данных "звезда"
- Созданы аналитические витрины
- Данные загружены в ClickHouse
- Проект полностью запускается через Docker

---
