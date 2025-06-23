# Car Rental Marketplace Big Data Analytics Pipeline on AWS

## Overview

This project outlines the design and implementation of a scalable, serverless big data analytics pipeline for a **car rental marketplace** using AWS services. The pipeline processes large volumes of transactional and operational data to generate actionable insights and key performance indicators (KPIs) for stakeholders. By leveraging **Amazon EMR Serverless**, **AWS Glue**, **Amazon Athena**, **AWS Step Functions**, and **Amazon S3**, this solution ensures efficient data ingestion, transformation, storage, and querying while adhering to data engineering best practices.

The pipeline automates the extraction, transformation, and loading (ETL) of raw data, computes business-critical metrics, and enables ad-hoc analysis to support decision-making related to platform performance, user engagement, and revenue trends.

---

## Problem Statement

The car rental marketplace generates massive datasets daily, including vehicle inventories, user profiles, rental transactions, and location metadata. Stakeholders require insights to:

- Evaluate **vehicle and location performance** (e.g., revenue and transaction volume by location).
- Analyze **user behavior** (e.g., spending patterns, rental frequency, and engagement).
- Monitor **revenue and transaction trends** over time to inform business strategies.

The challenge is to build a **fully automated, serverless, and resilient** data pipeline that processes raw data, computes KPIs, and enables SQL-based querying with minimal operational overhead.

---

## Objectives

The project aims to achieve the following:

1. Demonstrate the use of **Amazon EMR Serverless** for scalable Spark-based data processing.
2. Ingest and transform raw data stored in **Amazon S3** into optimized Parquet formats.
3. Compute **business metrics and KPIs** for vehicles, locations, users, and transactions.
4. Use **AWS Glue Crawlers** to infer schemas and populate the **AWS Glue Data Catalog**.
5. Enable **ad-hoc querying** with **Amazon Athena** for data exploration.
6. Orchestrate the pipeline with **AWS Step Functions**, incorporating logging, retries, and error handling.
7. Ensure **cost efficiency**, **scalability**, and **maintainability** through serverless AWS services.

---

## Datasets

Raw data is stored in Amazon S3 under the prefix `s3://car-rentals-data/raw/` in CSV format. The datasets include:

- **`vehicles.csv`**:
  - Columns: `vehicle_id`, `vehicle_type`, `make`, `model`, `year`, `daily_rate`
  - Description: Details of all rental vehicles available on the platform.
- **`users.csv`**:
  - Columns: `user_id`, `first_name`, `last_name`, `email`, `signup_date`, `city`
  - Description: User registration and profile information.
- **`locations.csv`**:
  - Columns: `location_id`, `city`, `state`, `country`, `zip_code`
  - Description: Metadata for pickup and drop-off locations.
- **`rental_transactions.csv`**:
  - Columns: `transaction_id`, `user_id`, `vehicle_id`, `pickup_location_id`, `dropoff_location_id`, `start_time`, `end_time`, `total_amount`
  - Description: Logs of rental transactions, including duration and payment details.

**Example Schema Preview**:

| Dataset                | Sample Columns                              | Format | S3 Path                           |
|------------------------|---------------------------------------------|--------|-----------------------------------|
| vehicles.csv           | vehicle_id, vehicle_type, daily_rate        | CSV    | s3://car-rentals-data/raw/vehicles/ |
| users.csv              | user_id, email, signup_date                 | CSV    | s3://car-rentals-data/raw/users/    |
| locations.csv          | location_id, city, zip_code                 | CSV    | s3://car-rentals-data/raw/locations/ |
| rental_transactions.csv| transaction_id, user_id, total_amount        | CSV    | s3://car-rentals-data/raw/transactions/ |

---

## Architecture

The pipeline follows a modular, serverless architecture to ensure scalability and ease of maintenance.

### Architecture Diagram
![alt text](docs/Lab4-Architecture.png)

### Key Components:

- **Amazon S3**:
  - Stores raw data (`raw/`), transformed data (`processed/`), Spark scripts (`scripts/`), and Athena query results (`query-results/`).
  - Uses partitioning for optimized storage and query performance.
- **Amazon EMR Serverless**:
  - Executes Apache Spark jobs for data transformation and KPI computation.
  - Eliminates the need for manual cluster management.
- **AWS Glue Crawlers**:
  - Automatically infers schemas from processed Parquet files.
  - Populates the **AWS Glue Data Catalog** for querying.
- **Amazon Athena**:
  - Enables SQL-based querying of processed data for ad-hoc analysis.
  - Integrates with the Glue Data Catalog for schema access.
- **AWS Step Functions**:
  - Orchestrates the ETL workflow, including parallel Spark job execution, crawler triggers, and Athena queries.
  - Implements retry logic, error handling, and logging for reliability.
- **Amazon CloudWatch**:
  - Monitors pipeline execution and logs errors for troubleshooting.

---

## Pipeline Workflow

### Step 1: Data Ingestion
- Raw CSV files are uploaded to `s3://car-rentals-data/raw/`.
- Files are organized by dataset type (e.g., `raw/vehicles/`, `raw/transactions/`).

### Step 2: Data Transformation with Spark on EMR Serverless
Two Spark scripts process the raw data:

- **`vehicle_location_metrics.py`**:
  - Joins `rental_transactions`, `vehicles`, and `locations` datasets.
  - Computes metrics such as:
    - Revenue and transaction count per location.
    - Average, max, and min transaction amounts.
    - Unique vehicles used per location.
    - Rental duration by vehicle type.
  - Outputs: Partitioned Parquet files in `s3://car-rentals-data/processed/vehicle_location_metrics/`.

- **`user_transaction_metrics.py`**:
  - Joins `rental_transactions` and `users` datasets.
  - Computes metrics such as:
    - Daily revenue and transaction count.
    - User-specific metrics (total spent, rental hours, transaction frequency).
    - Average, max, and min transaction amounts per user.
  - Outputs: Partitioned Parquet files in `s3://car-rentals-data/processed/user_transaction_metrics/`.

**Sample Spark Code Snippet** (from `vehicle_location_metrics.py`):
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, max, min, col, datediff

spark = SparkSession.builder.appName("VehicleLocationMetrics").getOrCreate()

# Read raw data
transactions = spark.read.csv("s3://car-rentals-data/raw/transactions/", header=True)
locations = spark.read.csv("s3://car-rentals-data/raw/locations/", header=True)
vehicles = spark.read.csv("s3://car-rentals-data/raw/vehicles/", header=True)

# Join and compute metrics
metrics = (transactions.join(locations, transactions.pickup_location_id == locations.location_id)
           .join(vehicles, transactions.vehicle_id == vehicles.vehicle_id)
           .groupBy("pickup_location_id", "city", "vehicle_type")
           .agg(
               sum("total_amount").alias("total_revenue"),
               count("transaction_id").alias("total_transactions"),
               avg("total_amount").alias("avg_transaction"),
               max("total_amount").alias("max_transaction"),
               min("total_amount").alias("min_transaction")
           ))

# Write to S3
metrics.write.partitionBy("city").parquet("s3://car-rentals-data/processed/vehicle_location_metrics/")
```

### Step 3: Schema Inference with AWS Glue Crawlers
Three Glue Crawlers are configured to scan the processed data:

- **Vehicle Location Metrics Crawler**: Scans `s3://car-rentals-data/processed/vehicle_location_metrics/`.
- **User Transaction Metrics Crawler**: Scans `s3://car-rentals-data/processed/user_transaction_metrics/`.
- **Daily Metrics Crawler**: Scans aggregated daily metrics.

Crawlers update the **Glue Data Catalog** with inferred schemas, enabling Athena queries.

### Step 4: Querying with Amazon Athena
Athena is used for ad-hoc and automated SQL queries. Example queries include:
![alt text](docs/Lab4-Athena_query.png)

- **Top 5 Revenue-Generating Locations**:
```sql
SELECT city, SUM(total_revenue) AS revenue
FROM vehicle_location_metrics
GROUP BY city
ORDER BY revenue DESC
LIMIT 5;
```

- **Most Rented Vehicle Type**:
```sql
SELECT vehicle_type, COUNT(*) AS rental_count
FROM vehicle_location_metrics
GROUP BY vehicle_type
ORDER BY rental_count DESC
LIMIT 1;
```

- **Top-Spending Users**:
```sql
SELECT user_id, SUM(total_spent) AS total_spent
FROM user_transaction_metrics
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;
```

Query results are saved to `s3://car-rentals-data/query-results/`.

### Step 5: Orchestration with AWS Step Functions
A Step Functions state machine automates the pipeline:

1. **Start Spark Jobs**: Triggers `vehicle_location_metrics.py` and `user_transaction_metrics.py` in parallel using EMR Serverless.
2. **Run Glue Crawlers**: Initiates crawlers after Spark jobs complete.
3. **Execute Athena Queries**: Runs predefined SQL queries for daily insights.
4. **Handle Errors**: Implements retry logic (up to 3 attempts) and fail states with CloudWatch logging.

![alt text](<docs/Lab4-Step function.png>)

**State Machine Definition** (simplified JSON):
```json
{
  "StartAt": "RunSparkJobs",
  "States": {
    "RunSparkJobs": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "VehicleLocationMetrics",
          "States": {
            "VehicleLocationMetrics": {
              "Type": "Task",
              "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
              "Parameters": {
                "ApplicationId": "emr-serverless-app-id",
                "ExecutionRoleArn": "arn:aws:iam::account-id:role/emr-serverless-role",
                "JobDriver": {
                  "SparkSubmit": {
                    "EntryPoint": "s3://car-rentals-data/scripts/vehicle_location_metrics.py"
                  }
                }
              },
              "Next": "VehicleLocationCrawler"
            },
            "VehicleLocationCrawler": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startCrawler.sync",
              "Parameters": { "Name": "vehicle-location-crawler" },
              "End": true
            }
          }
        },
        {
          "StartAt": "UserTransactionMetrics",
          "States": {
            "UserTransactionMetrics": {
              "Type": "Task",
              "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
              "Parameters": {
                "ApplicationId": "emr-serverless-app-id",
                "ExecutionRoleArn": "arn:aws:iam::account-id:role/emr-serverless-role",
                "JobDriver": {
                  "SparkSubmit": {
                    "EntryPoint": "s3://car-rentals-data/scripts/user_transaction_metrics.py"
                  }
                }
              },
              "Next": "UserTransactionCrawler"
            },
            "UserTransactionCrawler": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startCrawler.sync",
              "Parameters": { "Name": "user-transaction-crawler" },
              "End": true
            }
          }
        }
      ],
      "Next": "RunAthenaQueries"
    },
    "RunAthenaQueries": {
      "Type": "Task",
      "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
      "Parameters": {
        "QueryString": "SELECT city, SUM(total_revenue) AS revenue FROM vehicle_location_metrics GROUP BY city;",
        "WorkGroup": "primary",
        "ResultConfiguration": {
          "OutputLocation": "s3://car-rentals-data/query-results/"
        }
      },
      "End": true
    }
  }
}
```

---

## Key Metrics and KPIs

### Vehicle and Location Metrics
- **Total Revenue**: Sum of `total_amount` per location.
- **Total Transactions**: Count of transactions per location.
- **Average/Max/Min Transaction Amount**: Aggregated per location.
- **Unique Vehicles Used**: Distinct `vehicle_id` count per location.
- **Average/Total Rental Duration**: Calculated using `datediff(end_time, start_time)`.

### User and Daily Metrics
- **Daily Revenue**: Sum of `total_amount` per day.
- **Daily Transaction Count**: Count of transactions per day.
- **Total Spent per User**: Sum of `total_amount` per `user_id`.
- **Total Rental Hours per User**: Sum of rental durations.
- **Average/Max/Min Transaction Amount per User**: Aggregated per `user_id`.

---

## Optional Visualizations

To visualize KPIs, consider the following tools:

- **Amazon QuickSight**: For interactive dashboards (cost considerations apply).
- **Amazon Managed Grafana**: Free with CloudWatch integration for real-time monitoring.
- **Streamlit/Dash**: Python-based, self-hosted apps for custom dashboards.
- **Power BI/Tableau**: Connect to Athena for advanced visualizations.

---

## How to Reproduce

1. **Set Up S3 Buckets**:
   - Create `s3://car-rentals-data/` with prefixes: `raw/`, `processed/`, `scripts/`, `query-results/`.
   - Upload raw CSVs to `raw/` (e.g., `raw/vehicles/vehicles.csv`).

2. **Configure EMR Serverless**:
   - Create an EMR Serverless application in the AWS Console.
   - Assign an IAM role with permissions for S3, EMR, and CloudWatch.

3. **Upload Spark Scripts**:
   - Place `vehicle_location_metrics.py` and `user_transaction_metrics.py` in `s3://car-rentals-data/scripts/`.

4. **Deploy Step Functions**:
   - Create a state machine using the provided JSON definition.
   - Update ARNs for EMR Serverless, Glue crawlers, and S3 paths.

5. **Set Up Glue Crawlers**:
   - Create crawlers for `processed/vehicle_location_metrics/` and `processed/user_transaction_metrics/`.
   - Schedule or trigger via Step Functions.

6. **Query with Athena**:
   - Use the Glue Data Catalog as the data source.
   - Run sample queries to validate results.

7. **Run the Pipeline**:
   - Start the Step Functions state machine.
   - Monitor execution via CloudWatch logs.

---

## IAM Policies

The following IAM policies are required for the pipeline:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::car-rentals-data/*",
        "arn:aws:s3:::car-rentals-data"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "emr-serverless:StartJobRun",
        "emr-serverless:GetJobRun"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartCrawler",
        "glue:GetCrawler"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/stepfunctions/*"
    }
  ]
}
```

---

## Lessons Learned

- **EMR Serverless** simplifies Spark job execution by removing cluster management overhead.
- **Step Functions** provide clear visualization and control over complex workflows.
- **Partitioning** in Parquet files significantly improves query performance in Athena.
- **Glue Crawlers** automate schema management but require careful configuration to avoid redundant runs.
- **Error Handling** (e.g., retries, fail states) is critical for production-grade pipelines.
- **CloudWatch Logs** are essential for debugging and monitoring pipeline execution.

---

## Future Enhancements

- Integrate **AWS Lambda** for event-driven triggers (e.g., new file uploads to S3).
- Implement **machine learning models** (e.g., demand forecasting) using SageMaker.
- Add **real-time streaming** with Amazon Kinesis for live transaction processing.
- Enhance visualizations with **QuickSight** or **Grafana** for stakeholder dashboards.

---

## Contributors

- **Hakeem** – Data Engineer & Architect
- Special thanks to the AWS community and documentation for guidance.

