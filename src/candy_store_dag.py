import os
import sys
import shutil
import logging
from airflow import DAG
from typing import Dict
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from multiprocessing import Process, set_start_method

set_start_method("spawn", force=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Airflow-specific path configuration
PROJECT_PATH = os.environ.get(
    "PROJECT_PATH", "<Project-Root-Path>"  # Replace with path to project root folder
)

# Check if the user has set the PROJECT_PATH correctly
if PROJECT_PATH == "<Project-Root-Path>" or not PROJECT_PATH:
    raise ValueError(
        "PROJECT_PATH environment variable is not set or still contains the placeholder '<Project-Root-Path>'. "
        "Please set it to the absolute path of your project root folder (e.g., '/home/user/candy_store_project')."
    )
if not os.path.exists(PROJECT_PATH):
    raise ValueError(
        f"PROJECT_PATH '{PROJECT_PATH}' does not exist. Please set a valid directory path."
    )

SRC_PATH = os.path.join(PROJECT_PATH, "src")

# Add to Python path before any imports
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
    sys.path.insert(0, PROJECT_PATH)
from data_processor import DataProcessor

load_dotenv(dotenv_path=f"{PROJECT_PATH}/.env", override=True)


def create_spark_session(app_name: str = "CandyStoreAnalytics") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.mongodb.input.uri", os.getenv("MONGODB_URI"))
        .config("spark.checkpoint.dir", "/tmp/checkpoint/dir")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark


# Utility to generate a list of dates (YYYYMMDD) between two dates
def get_date_range(start_date: str, end_date: str) -> list:
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return date_list


# Task 1: Load configuration from environment and generate a date range
def load_config() -> Dict:
    """Load configuration from environment variables"""
    return {
        "mongodb_uri": os.getenv("MONGODB_URI"),
        "mongodb_db": os.getenv("MONGO_DB"),
        "mongodb_collection_prefix": os.getenv("MONGO_COLLECTION_PREFIX"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "mysql_db": os.getenv("MYSQL_DB"),
        "customers_table": os.getenv("CUSTOMERS_TABLE"),
        "products_table": os.getenv("PRODUCTS_TABLE"),
        "output_path": os.getenv("OUTPUT_PATH"),
        "reload_inventory_daily": os.getenv("RELOAD_INVENTORY_DAILY", "false").lower()
        == "true",
    }


def load_configuration(**kwargs):
    config = load_config()
    date_range = get_date_range(
        os.getenv("MONGO_START_DATE"), os.getenv("MONGO_END_DATE")
    )
    # Push configuration and date range to XCom for downstream tasks
    kwargs["ti"].xcom_push(key="config", value=config)
    kwargs["ti"].xcom_push(key="date_range", value=date_range)
    print("Configuration loaded.")
    return "Configuration loaded."


# Task 2: Process orders in batches and save a daily summary as an intermediate CSV
def process_orders(**kwargs):
    spark = create_spark_session(app_name="ProcessOrders")
    data_processor = DataProcessor(spark)

    ti = kwargs["ti"]
    config = ti.xcom_pull(key="config", task_ids="load_configuration")
    date_range = ti.xcom_pull(key="date_range", task_ids="load_configuration")

    try:
        data_processor.configure(config, date_range)
        print("Starting batch processing of orders...")
        data_processor.process_batches(date_range)

        # temp_path = "/tmp/daily_summary.parquet"
        temp_path = os.path.join(
            config["output_path"], "daily_summary.parquet"
        )  # Use configured output path
        data_processor.daily_summary_df.write.parquet(temp_path, mode="overwrite")
        kwargs["ti"].xcom_push(key="temp_path", value=temp_path)
    finally:
        spark.stop()
        logging.info("Spark session stopped in process_orders")

    return "Order processing complete."


# Task 3: Forecast sales and profits based on the daily summary, then output forecasting metrics
def forecast_sales(**kwargs):
    ti = kwargs["ti"]
    temp_path = ti.xcom_pull(key="temp_path", task_ids="process_orders")
    config = ti.xcom_pull(key="config", task_ids="load_configuration")

    # Create and start isolated process
    p = Process(
        target=_run_forecast, args=(temp_path, config), name="prophet-forecast-process"
    )
    p.start()
    p.join()  # Wait for completion
    logging.info("Forecast process completed")


def _run_forecast(temp_path: str, config: dict):
    """Isolated Prophet execution environment"""
    spark = SparkSession.builder.appName("ForecastSubprocess").getOrCreate()

    try:
        # Re-initialize DataProcessor inside child process
        from data_processor import DataProcessor

        data_processor = DataProcessor(spark)

        # Load data from shared path
        daily_summary_df = spark.read.parquet(temp_path)

        # Run forecasting
        forecast_df = data_processor.forecast_sales_and_profits(daily_summary_df)

        # Save results
        if forecast_df is not None:
            data_processor.save_to_csv(
                forecast_df, config["output_path"], "sales_profit_forecast.csv"
            )
            logging.info("Forecast saved successfully")

    except Exception as e:
        logging.error(f"Forecast failed: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()
        logging.info("Spark session terminated in subprocess")


default_args = {
    "owner": "airflow",  # Who owns/maintains this DAG
    "depends_on_past": False,  # Tasks don't depend on past runs
    "start_date": datetime(2025, 3, 6),  # When the DAG should start running
    "email_on_failure": False,  # Don't send emails on task failure
    "email_on_retry": False,  # Don't send emails on task retries
    "retries": 1,  # Number of times to retry a failed task
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
}

# Define the DAG
dag = DAG(
    "candy_store_dag",
    default_args=default_args,
    description="DAG to process orders: load config, import MySQL/MongoDB data, process batches, and forecast sales/profits",
    schedule_interval="@daily",
    concurrency=1,
    catchup=False,
)

load_config_task = PythonOperator(
    task_id="load_configuration", python_callable=load_configuration, dag=dag
)

process_orders_task = PythonOperator(
    task_id="process_orders", python_callable=process_orders, dag=dag
)

forecast_sales_task = PythonOperator(
    task_id="forecast_sales", python_callable=forecast_sales, dag=dag
)

# Task dependencies
load_config_task >> process_orders_task >> forecast_sales_task

if __name__ == "__main__":
    dag.cli()
