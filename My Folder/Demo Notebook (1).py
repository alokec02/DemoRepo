# Databricks notebook source
# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.help()

# COMMAND ----------

# MAGIC %python
# MAGIC files = dbutils.fs.ls('/databricks-datasets')
# MAGIC display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employees
# MAGIC   (id INT, name STRING, salary DOUBLE);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC (1, "Adam", 3500.0),
# MAGIC (2, "Sarah", 4020.5),
# MAGIC (3, "John", 2999.3),
# MAGIC (4, "Thomas", 4000.3),
# MAGIC (5, "Anna", 2500.0),
# MAGIC (6, "Kim", 6200.3)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees 
# MAGIC SET salary = salary + 100
# MAGIC WHERE name LIKE "A%"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employees VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees@v1

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE employees TO VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE employees
# MAGIC ZORDER by id

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE employees

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE managed_default
# MAGIC (width INT, length INT, height INT);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO managed_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_default

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_default
# MAGIC (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_default'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO external_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC USE new_default;
# MAGIC
# MAGIC CREATE TABLE managed_new_default
# MAGIC (width INT, length INT, height INT);
# MAGIC
# MAGIC INSERT INTO managed_new_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);
# MAGIC
# MAGIC CREATE TABLE external_new_default
# MAGIC (width INT, length INT, height INT)
# MAGIC Location 'dbfs:/mnt/demo/external_new_default';
# MAGIC
# MAGIC INSERT INTO external_new_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_new_default;
# MAGIC DROP TABLE external_new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA Custom
# MAGIC Location 'dbfs:/Shared/schemas/custom.db'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED Custom

# COMMAND ----------

# MAGIC %sql
# MAGIC USE Custom;
# MAGIC  
# MAGIC CREATE TABLE managed_new_default
# MAGIC (width INT, length INT, height INT);
# MAGIC  
# MAGIC INSERT INTO managed_new_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);
# MAGIC  
# MAGIC CREATE TABLE external_new_default
# MAGIC (width INT, length INT, height INT)
# MAGIC Location 'dbfs:/mnt/demo/external_new_default';
# MAGIC  
# MAGIC INSERT INTO external_new_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS smartphones
# MAGIC (id INT, name STRING, brand STRING, year INT);
# MAGIC
# MAGIC INSERT INTO smartphones
# MAGIC VALUES (1, 'iPhone 14', 'Apple', 2022),
# MAGIC       (2, 'iPhone 13', 'Apple', 2021),
# MAGIC       (3, 'iPhone 6', 'Apple', 2014),
# MAGIC       (4, 'iPad Air', 'Apple', 2013),
# MAGIC       (5, 'Galaxy S22', 'Samsung', 2022),
# MAGIC       (6, 'Galaxy Z Fold', 'Samsung', 2022),
# MAGIC       (7, 'Galaxy S9', 'Samsung', 2016),
# MAGIC       (8, '12 Pro', 'Xiaomi', 2022),
# MAGIC       (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
# MAGIC       (10, 'Redmi Note 11', 'Xiaomi', 2021)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW view_apple_phones
# MAGIC AS SELECT *
# MAGIC FROM smartphones
# MAGIC WHERE brand = 'Apple';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM view_apple_phones

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW temp_view_phone_brands
# MAGIC AS SELECT DISTINCT brand
# MAGIC FROM smartphones;
# MAGIC
# MAGIC SELECT * FROM temp_view_phone_brands;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
# MAGIC AS SELECT * FROM smartphones
# MAGIC WHERE year > 2020
# MAGIC ORDER BY year DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.global_temp_view_latest_phones;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %run  raw.githubusercontent.com_derar-alhussein_Databricks-Certified-Data-Engineer-Associate_main_Includes_Copy-Datasets

# COMMAND ----------

# MAGIC %python
# MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")

# COMMAND ----------

# MAGIC %python
# MAGIC display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM json.`${dataset.bookstore}/customers-json/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC   input_file_name() source_file
# MAGIC FROM json.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM text.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`${dataset.bookstore}/books-csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE books_csv
# MAGIC (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC )
# MAGIC LOCATION "${dataset.bookstore}/books-csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED  books_csv

# COMMAND ----------

# MAGIC %python
# MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
# MAGIC display(files)
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC (spark.read
# MAGIC .table("books_csv")
# MAGIC .write
# MAGIC .mode("append")
# MAGIC .format("csv")
# MAGIC .option('header', 'true')
# MAGIC .option('delimiter', ';')
# MAGIC .save(f"{dataset_bookstore}/books-csv")
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
# MAGIC display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM books_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE books_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM books_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE orders AS
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`;    
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders AS
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`; 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE orders
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customers_updates AS 
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;
# MAGIC
# MAGIC MERGE INTO customers c
# MAGIC USING customers_updates u
# MAGIC ON c.customer_id = u.customer_id
# MAGIC WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
# MAGIC   UPDATE SET email = u.email, updated = u.updated
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_updates
# MAGIC    (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "${dataset.bookstore}/books-csv-new",
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_updates
# MAGIC

# COMMAND ----------

# MAGIC %run raw.githubusercontent.com_derar-alhussein_Databricks-Certified-Data-Engineer-Associate_main_Includes_Copy-Datasets 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customers AS
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json`;
# MAGIC
# MAGIC SELECT * FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, profile:first_name, profile:address:country 
# MAGIC FROM customers
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_customers AS
# MAGIC   SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) AS profile_struct
# MAGIC   FROM customers;
# MAGIC   
# MAGIC SELECT * FROM parsed_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customers_final AS
# MAGIC   SELECT customer_id, profile_struct.*
# MAGIC   FROM parsed_customers;
# MAGIC   
# MAGIC SELECT * FROM customers_final

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW books_tmp_vw
# MAGIC    (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "${dataset.bookstore}/books-csv/export_*.csv",
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE books AS
# MAGIC   SELECT * FROM books_tmp_vw;
# MAGIC   
# MAGIC SELECT * FROM books

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW orders_enriched AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT *, explode(books) AS book 
# MAGIC   FROM orders) o
# MAGIC INNER JOIN books b
# MAGIC ON o.book.book_id = b.book_id;
# MAGIC
# MAGIC SELECT * FROM orders_enriched
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     book.book_id AS book_id,
# MAGIC     book.quantity AS quantity
# MAGIC   FROM orders_enriched
# MAGIC ) PIVOT (
# MAGIC   sum(quantity) FOR book_id in (
# MAGIC     'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
# MAGIC     'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
# MAGIC   )
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   books,
# MAGIC   FILTER (books, i -> i.quantity >= 2) AS multiple_copies
# MAGIC FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, multiple_copies
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     order_id,
# MAGIC     FILTER (books, i -> i.quantity >= 2) AS multiple_copies
# MAGIC   FROM orders)
# MAGIC WHERE size(multiple_copies) > 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   books,
# MAGIC   TRANSFORM (
# MAGIC     books,
# MAGIC     b -> CAST(b.subtotal * 0.8 AS INT)
# MAGIC   ) AS subtotal_after_discount
# MAGIC FROM orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_url(email STRING)
# MAGIC RETURNS STRING
# MAGIC
# MAGIC RETURN concat("https://www.", split(email, "@")[1])
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, get_url(email) domain
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION site_type(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC           WHEN email like "%.com" THEN "Commercial business"
# MAGIC           WHEN email like "%.org" THEN "Non-profits organization"
# MAGIC           WHEN email like "%.edu" THEN "Educational institution"
# MAGIC           ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
# MAGIC        END;
# MAGIC
# MAGIC SELECT email, site_type(email) as domain_category
# MAGIC FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION get_url;
# MAGIC DROP FUNCTION site_type;

# COMMAND ----------

# MAGIC %run  raw.githubusercontent.com_derar-alhussein_Databricks-Certified-Data-Engineer-Associate_main_Includes_Copy-Datasets

# COMMAND ----------

# MAGIC %python
# MAGIC (spark.readStream
# MAGIC       .table("books")
# MAGIC       .createOrReplaceTempView("books_streaming_tmp_vw")
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC (spark.table("author_counts_tmp_vw")                               
# MAGIC       .writeStream  
# MAGIC       .trigger(processingTime='4 seconds')
# MAGIC       .outputMode("complete")
# MAGIC       .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
# MAGIC       .table("author_counts")
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC        ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC        ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC  values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC          ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC          ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

# MAGIC %python
# MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")

# COMMAND ----------

display(files)

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP Table orders_updates

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
# MAGIC SELECT *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC FROM orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_tmp

# COMMAND ----------

(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
      .outputMode("append")
      .table("orders_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

(spark.read
      .format("json")
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup"))


# COMMAND ----------

(spark.readStream
  .table("orders_bronze")
  .createOrReplaceTempView("orders_bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC FROM orders_bronze_tmp o
# MAGIC INNER JOIN customers_lookup c
# MAGIC ON o.customer_id = c.customer_id
# MAGIC WHERE quantity > 0)

# COMMAND ----------

(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
      .outputMode("append")
      .table("orders_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

(spark.readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC FROM orders_silver_tmp
# MAGIC GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
      .trigger(availableNow=True)
      .table("daily_customer_books"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books

# COMMAND ----------

load_new_data(all=True)

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
