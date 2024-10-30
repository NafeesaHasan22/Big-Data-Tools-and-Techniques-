-- Databricks notebook source
-- MAGIC %md #####LOAD AND PREPARE THE DATASETS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_location= "/FileStore/tables/clinicaltrial_2023.csv"
-- MAGIC
-- MAGIC clinicaltrial_2023_df = spark.read.text(file_location)
-- MAGIC
-- MAGIC clinicaltrial_2023_df = clinicaltrial_2023_df.selectExpr("split(value, '\t') as data")
-- MAGIC
-- MAGIC for i in range(14):  # Assuming there are 14 columns
-- MAGIC   clinicaltrial_2023_df = clinicaltrial_2023_df.withColumn(f"col_{i+1}", clinicaltrial_2023_df.data[i])
-- MAGIC
-- MAGIC clinicaltrial_2023_df = clinicaltrial_2023_df.drop("data")
-- MAGIC
-- MAGIC display(clinicaltrial_2023_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma_df = spark.read.csv("/FileStore/tables/pharma.csv", header=True, inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2023_df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma_df.show(truncate=False)

-- COMMAND ----------

-- MAGIC %md #####FILTER THE HEADER ROWS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2023_df1 = clinicaltrial_2023_df.filter((clinicaltrial_2023_df['col_1'] != '"Id') & (clinicaltrial_2023_df['col_2'] != 'Study Title') & (clinicaltrial_2023_df['col_3'] != 'Acronym') & (clinicaltrial_2023_df['col_4'] != 'Status') & (clinicaltrial_2023_df['col_5'] != 'Conditions') & (clinicaltrial_2023_df['col_6'] != 'Interventions') & (clinicaltrial_2023_df['col_7'] != 'Sponsor') & (clinicaltrial_2023_df['col_8'] != 'Collaborators') & (clinicaltrial_2023_df['col_9'] != 'Enrollment') & (clinicaltrial_2023_df['col_10'] != 'Funder Type') & (clinicaltrial_2023_df['col_11'] != 'Type') & (clinicaltrial_2023_df['col_12'] != 'Study Design') & (clinicaltrial_2023_df['col_13'] != 'Start') & (clinicaltrial_2023_df['col_14'] != 'Completion'))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2023_df1.show()

-- COMMAND ----------

-- MAGIC %md #####CLEAN DATAFRAME

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC from pyspark.sql.functions import regexp_replace
-- MAGIC
-- MAGIC # Remove comma and '|' symbol from all columns in the DataFrame
-- MAGIC clinicaltrial_2023_df2 = clinicaltrial_2023_df1.select(*[regexp_replace(col(c), '[,]', '').alias(c) for c in clinicaltrial_2023_df.columns])
-- MAGIC
-- MAGIC # Show the cleaned DataFrame
-- MAGIC clinicaltrial_2023_df2.show(truncate=False)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md #####SELECTED COLUMNS FOR ANALYSIS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC clinicaltrial_2023_df3 = clinicaltrial_2023_df2.select(
-- MAGIC     col("col_1").alias("Id"),
-- MAGIC     col("col_4").alias("Status"),
-- MAGIC     col("col_5").alias("Conditions"),
-- MAGIC     col("col_7").alias("Sponsor"),
-- MAGIC     col("col_11").alias("Type"),
-- MAGIC     col("col_12").alias("Study Design"),
-- MAGIC     col("col_13").alias("Start"),
-- MAGIC     col("col_14").alias("Completion"))
-- MAGIC pharma_df1 = pharma_df.select(col("Parent_Company"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2023_df3.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma_df1.show()

-- COMMAND ----------

-- MAGIC %md #####CREATE DATABASE AND TABLES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicaltrial_2023_df2.createOrReplaceTempView("clinicaltrial_2023")
-- MAGIC pharma_df1.createOrReplaceTempView("pharma")

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE TABLE default.clinicaltrial_2023 AS SELECT * FROM clinicaltrial_2023

-- COMMAND ----------

CREATE OR REPLACE TABLE default.pharma AS SELECT * FROM pharma

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md #####CREATE DATABASE clinicaltrialAnalysis

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ClinicaltrialAnalysis

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

-- MAGIC %md #####CREATE TABLES clinicaltrial_2023 & pharma

-- COMMAND ----------

CREATE OR REPLACE TABLE ClinicaltrialAnalysis.clinicaltrial_2023 AS SELECT * FROM clinicaltrial_2023

-- COMMAND ----------

CREATE OR REPLACE TABLE ClinicaltrialAnalysis.pharma AS SELECT * FROM pharma

-- COMMAND ----------

SHOW TABLES IN ClinicaltrialAnalysis

-- COMMAND ----------

-- MAGIC %md #####TASK 1 : NUMBER OF STUDIES IN THE DATASET

-- COMMAND ----------

-- 1. The number of studies in the dataset (distinct studies)
SELECT COUNT(DISTINCT col_1) AS num_distinct_studies
FROM clinicaltrial_2023;

-- COMMAND ----------

-- MAGIC %md #####TASK 2 : TYPES OF STUDIES IN THE DATASET ALONG WITH THE FREQUENCIES OF EACH TYPE ORDERED FROM MOST FREQUENT TO LEAST FREQUENT 

-- COMMAND ----------

-- 2. List all types of studies with frequencies ordered from most to least frequent
SELECT col_11, COUNT(*) AS freq
FROM clinicaltrial_2023
GROUP BY col_11
ORDER BY freq DESC;

-- COMMAND ----------

-- MAGIC %md #####TASK 3 : TOP 5 CONDITIONS WITH THEIR FREQUENCIES

-- COMMAND ----------

-- Step 1: Split the Conditions column by '|'
CREATE OR REPLACE TEMP VIEW split_conditions_view AS
SELECT explode(split(col_5, '\\|')) AS Condition
FROM clinicaltrial_2023;

-- Step 2 & 3: Group by the Condition column and count occurrences, order by count
-- Step 4: Limit the result to the top 5 conditions
SELECT Condition, COUNT(*) AS count
FROM split_conditions_view
GROUP BY Condition
ORDER BY count DESC
LIMIT 5;


-- COMMAND ----------

-- MAGIC %md #####TASK 4 : 10 MOST COMMON SPONSORS THAT ARE NOR PHARMACEUTICAL COMPANIES, ALONG WITH NUMBER OF CLINICAL TRIALS THEY HAVE SPONSORED

-- COMMAND ----------

-- Step 1: Get distinct pharmaceutical companies from pharma_df DataFrame
CREATE OR REPLACE TEMP VIEW pharma_companies_view AS
SELECT DISTINCT Parent_Company
FROM pharma;

-- Step 2: Join clinicaltrial_2023_df2 DataFrame with pharma_companies DataFrame to filter out pharmaceutical sponsors
--          and count occurrences of each sponsor
CREATE OR REPLACE TEMP VIEW non_pharma_sponsors_view AS
SELECT c.col_7, COUNT(*) AS count
FROM clinicaltrial_2023 c
LEFT ANTI JOIN pharma_companies_view p ON c.col_7 = p.Parent_Company
GROUP BY c.col_7
ORDER BY count DESC
LIMIT 10;

-- Step 3: Print the top 10 non-pharmaceutical sponsors
SELECT * FROM non_pharma_sponsors_view;


-- COMMAND ----------

-- MAGIC %md #####TASK 5 : PLOT NUMBER OF COMPLETED STUDIES FOR EACH MONTH IN 2023

-- COMMAND ----------

-- Filter completed studies from clinicaltrial_2023_df2 DataFrame where col_14 is not null, not empty, status is 'COMPLETED', and col_14 contains '2023'
CREATE OR REPLACE TEMP VIEW completed_studies_view AS
SELECT *
FROM clinicaltrial_2023
WHERE col_14 IS NOT NULL AND 
      col_14 != '' AND 
      col_4 = 'COMPLETED' AND 
      col_14 LIKE '%2023%';

-- Extract month from the 'col_14' (Completion) column and count 1 for each month
CREATE OR REPLACE TEMP VIEW completed_month_counts_view AS
SELECT SUBSTRING(col_14, 1, 7) AS Month, COUNT(*) AS count
FROM completed_studies_view
GROUP BY SUBSTRING(col_14, 1, 7);

-- Collect the results
SELECT * FROM completed_month_counts_view;


-- COMMAND ----------

-- MAGIC %md #####VISUALIZATION FOR NUMBER OF COMPLETED STUDIES FOR EACH MONTH IN 2023

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Plotting the data
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC # Collecting the results into a list of dictionaries
-- MAGIC completed_month_counts = spark.sql("SELECT * FROM completed_month_counts_view").collect()
-- MAGIC months = [row['Month'] for row in completed_month_counts]
-- MAGIC counts = [row['count'] for row in completed_month_counts]
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(months, counts, color='blue')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Number of Completed Studies')
-- MAGIC plt.title('Number of Completed Studies for Each Month in 2023')
-- MAGIC plt.xticks(rotation=45)
-- MAGIC plt.grid(axis='y')
-- MAGIC plt.show()
-- MAGIC # Displaying the values plotted for each month
-- MAGIC print("Values plotted for each month:")
-- MAGIC for row in completed_month_counts:
-- MAGIC     print(row['Month'], "-", row['count'])
-- MAGIC

-- COMMAND ----------

-- MAGIC %md #####FUTHER ANALYSIS 

-- COMMAND ----------

-- MAGIC %md #####ANALYSIS 3 : COUNT THE OCCURRENCES OF COMPLETION YEARS BETWEEN 2018 & 2024 WITH STATUS "RECRUITING"

-- COMMAND ----------

-- Create or replace the temporary view with the necessary transformation
CREATE OR REPLACE TEMP VIEW clinicaltrial AS
SELECT *,
       CASE
           WHEN col_4 = 'RECRUITING' AND year(col_14) BETWEEN 2018 AND 2024 THEN 1
           ELSE 0
       END AS Recruiting 
FROM clinicaltrial_2023;

SELECT COUNT(*) AS count
FROM clinicaltrial
WHERE Recruiting  = 1;

