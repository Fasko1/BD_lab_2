# Big Data Spark — Лабораторная работа №2
## ETL-пайплайн на Apache Spark с JDBC-интеграцией

---

## 📌 Описание проекта

В рамках лабораторной работы реализован ETL-пайплайн на Apache Spark для обработки больших данных.

Пайплайн выполняет следующие этапы:

1. Чтение исходных CSV-файлов средствами Spark  
2. Загрузка данных в PostgreSQL (`mock_data`) через Spark JDBC  
3. Построение модели данных «звезда» в PostgreSQL  
4. Чтение модели «звезда» из PostgreSQL через Spark JDBC  
5. Формирование аналитических витрин средствами Spark  
6. Загрузка витрин в ClickHouse через Spark JDBC  

---
## Главное требование лабораторной

Основная сложность лабораторной работы — интеграция Spark с базами данных.

В данной работе это требование выполнено следующим образом:

- Spark читает и пишет в PostgreSQL через JDBC
- Spark читает и пишет в ClickHouse через JDBC
- PostgreSQL JDBC driver и ClickHouse JDBC driver добавлены внутрь Spark-контейнера через `Dockerfile.spark`

То есть Spark взаимодействует с базами данных напрямую, а не через обычные Python-вставки.


├── data/                           # 10 CSV-файлов mock_data
├── sql/
│   ├── init_postgres.sql           # создание таблицы mock_data
│   └── create_star_schema.sql      # создание схемы звезда
├── Dockerfile.spark                # образ Spark с JDBC-драйверами
├── docker-compose.yml
├── etl_to_star.py                  # ETL: CSV -> PostgreSQL (mock_data + star schema)
├── marts_to_clickhouse.py          # ETL: PostgreSQL star schema -> ClickHouse marts
├── requirements.txt
└── README.md

## ⚙️ Ключевая особенность

Интеграция с базами данных реализована через JDBC:

- PostgreSQL JDBC Driver  
- ClickHouse JDBC Driver  

Драйверы:
- добавлены в Spark-контейнер через `Dockerfile.spark`
- подключены через:
  - `spark.driver.extraClassPath`
  - `spark.executor.extraClassPath`

Spark напрямую работает с БД:

```python
spark.read.jdbc(...)
df.write.jdbc(...)
```

---

## 🧱 Используемые технологии

- Apache Spark 3.5.3  
- PostgreSQL 16  
- ClickHouse  
- Docker / Docker Compose  
- Python / PySpark  

### JDBC drivers:
- PostgreSQL JDBC Driver  
- ClickHouse JDBC Driver  

---

## 🐳 Состав контейнеров

Проект запускается через `docker compose`:

- **postgres** — хранение исходных данных и модели «звезда»  
- **clickhouse** — хранение аналитических витрин  
- **spark** — выполнение ETL + JDBC драйверы  

---

## 📂 Исходные данные

Используются файлы `MOCK_DATA*.csv`.

### Требования:
- 10 файлов  
- по 1000 строк  
- всего 10000 строк в `mock_data`  

---

## ⚠️ Особенности данных

Данные синтетические и высокоразнообразные.

В результате:

- `dim_stores` и `dim_suppliers` почти равны по размеру факту  
- это нормально и не ошибка  

---

## 🧩 Модель данных (PostgreSQL)

### Таблица фактов:
- `fact_sales`

### Таблицы измерений:
- `dim_customers`
- `dim_sellers`
- `dim_products`
- `dim_stores`
- `dim_suppliers`
- `dim_dates`

---

## 📊 Аналитические витрины (ClickHouse)

- `mart_sales_products`
- `mart_sales_customers`
- `mart_sales_time`
- `mart_sales_stores`
- `mart_sales_suppliers`
- `mart_product_quality`

---

## 🧠 Логика витрин

### 1. mart_sales_products
- топ-10 продуктов  
- выручка, продажи, рейтинг  

### 2. mart_sales_customers
- топ-10 клиентов  
- средний чек  

### 3. mart_sales_time
- тренды по месяцам и годам  

### 4. mart_sales_stores
- топ-5 магазинов  

### 5. mart_sales_suppliers
- топ-5 поставщиков  

### 6. mart_product_quality
- рейтинг + продажи  
- `revenue_per_review`

---

## ⚙️ Подготовка

Перед запуском:

- положить CSV в `data/`
- убедиться, что есть:
  - `Dockerfile.spark`
  - `docker-compose.yml`
  - `etl_to_star.py`
  - `marts_to_clickhouse.py`
  - `sql/`

---

## 🚀 Запуск проекта

### 1. Поднять контейнеры

```bash
docker compose down -v
docker compose up --build -d
```

---

### 2. ETL → PostgreSQL

```bash
docker exec -it bd_spark spark-submit etl_to_star.py
```

Скрипт выполняет:

- чтение CSV  
- загрузку в `public.mock_data` через Spark JDBC  
- построение измерений  
- построение `fact_sales`  
- загрузку звезды в PostgreSQL  

Результат:

- `public.mock_data: 10000`  
- `public.fact_sales: 10000`  

---

### 3. Витрины → ClickHouse

```bash
docker exec -it bd_spark spark-submit marts_to_clickhouse.py
```

Скрипт выполняет:

- чтение star schema через JDBC  
- объединение данных  
- расчет витрин  
- загрузку в ClickHouse  

---

## 🔍 Проверка

### PostgreSQL

```sql
SELECT COUNT(*) FROM public.mock_data;
SELECT COUNT(*) FROM public.fact_sales;
```

### ClickHouse

```sql
SELECT count() FROM marts.mart_sales_products;
SELECT count() FROM marts.mart_sales_customers;
```

---

## 🔌 JDBC-интеграция

В работе:

- драйверы лежат внутри Spark-контейнера  
- Spark использует их напрямую  
- чтение и запись выполняются самим Spark  

👉 Это соответствует требованиям лабораторной  

---

## ✅ Что реализовано

- ETL на Spark  
- модель «звезда»  
- 6 витрин  
- интеграция через JDBC  
- Docker окружение  

---

## ❌ Не реализовано (опционально)

- Cassandra  
- Neo4j  
- MongoDB  
- Valkey  

---

## 🏁 Итог

Реализован ETL-пайплайн, который:

- читает CSV  
- строит star schema  
- формирует витрины  
- пишет в ClickHouse  
- использует JDBC  

Проект полностью готов к проверке.
