import gc
import os
import glob
import shutil
import numpy as np
import concurrent.futures
from datetime import datetime
from typing import Dict, Tuple
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql.types import DecimalType
from time_series import ProphetForecaster
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode,
    col,
    round as spark_round,
    sum as spark_sum,
    count,
    abs as spark_abs,
    to_timestamp,
    to_date,
    countDistinct,
    coalesce,
    lit,
    when,
    broadcast,
    max as spark_max,
    first,
    lag,
    greatest,
    date_format,
)


class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Initialize all class properties
        self.__config = None
        self.current_inventory = None
        self.inventory_initialized = False
        self.original_products_df = None  # Store original products data
        self.reload_inventory_daily = False  # New flag for inventory reload
        self.order_items = None
        self.products_df = None
        self.customers_df = None
        self.transactions_df = None
        self.orders_df = None
        self.order_line_items_df = None
        self.daily_summary_df = None
        self.total_cancelled_items = 0

    def load_mysql_data(
        self, jdbc_url: str, db_table: str, db_user: str, db_password: str
    ) -> DataFrame:
        """
        Load data from MySQL database.

        :param jdbc_url: JDBC URL for the MySQL database
        :param db_table: Name of the table to load data from
        :param db_user: Database username
        :param db_password: Database password
        :return: DataFrame containing the loaded MySQL data
        """
        return (
            self.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", db_table)
            .option("user", db_user)
            .option("password", db_password)
            .load()
        )

    def load_mongo_data(self, db_name: str, collection_name: str) -> DataFrame:
        """
        Load data from MongoDB.

        :param db_name: Name of the MongoDB database
        :param collection_name: Name of the collection to load data from
        :return: DataFrame containing the loaded MongoDB data
        """
        mongo_data = (
            self.spark.read.format("mongo")
            .option("database", db_name)
            .option("collection", collection_name)
            .load()
        )

        return mongo_data

    def load_all_data(self, dates: list) -> None:

        # Threads to load MySQL data
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_customers = executor.submit(
                self.load_mysql_data,
                self.__config["mysql_url"],
                self.__config["customers_table"],
                self.__config["mysql_user"],
                self.__config["mysql_password"],
            )
            future_products = executor.submit(
                self.load_mysql_data,
                self.__config["mysql_url"],
                self.__config["products_table"],
                self.__config["mysql_user"],
                self.__config["mysql_password"],
            )

            # Retrieve the results; each future corresponds to a separate table
            self.customers_df = future_customers.result()
            self.original_products_df = future_products.result()

            executor.shutdown()

        # Threads to load MongoDB data
        dataframes = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit a load task for each date/collection
            futures = [
                executor.submit(
                    self.load_mongo_data,
                    self.__config["mongodb_db"],
                    f"{self.__config['mongodb_collection_prefix']}{date}",
                )
                for date in dates
            ]

            # Collect results while maintaining the original order
            for future in futures:
                try:
                    df = future.result()
                    dataframes.append(df)
                except Exception as e:
                    print(f"Error loading data: {e}")
            executor.shutdown()

        # Combine DataFrames and Order by Timestamp
        if dataframes:
            combined_df = dataframes[0]
            for df in dataframes[1:]:
                combined_df = combined_df.union(df)
            self.transactions_df = (
                combined_df.withColumn("timestamp", to_timestamp(col("timestamp")))
                .withColumn("date", to_date(col("timestamp")))
                .orderBy(col("timestamp").asc())
            )

            # Explode items to process each order line individually
            self.transactions_df = (
                self.transactions_df.withColumn("item", explode("items"))
                .drop("items")
                .drop("_id")
            )
            self.transactions_df = (
                self.transactions_df.withColumn("product_id", col("item.product_id"))
                .withColumn("product_name", col("item.product_name"))
                .withColumn("qty", col("item.qty").cast("int"))
                .where(col("qty").isNotNull())
                .drop("item")
                .repartition(100, "product_id")
            )  # Exclude invalid quantities

        else:
            self.transactions_df = self.spark.createDataFrame([], schema=None)

    def configure(self, config: Dict, date_range: list) -> None:
        """Configure the data processor with environment settings"""
        self.__config = config
        self.reload_inventory_daily = self.__config.get("reload_inventory_daily", False)
        print("\nINITIALIZING DATA SOURCES")
        print("-" * 80)
        if self.reload_inventory_daily:
            print("Daily inventory reload: ENABLED")
        else:
            print("Daily inventory reload: DISABLED")

        self.load_all_data(date_range)

        # print('Customers Table:')
        # self.customers_df.show()

        # print('Products Table:')
        # self.products_df.show()

        # print('Transactions Table:')
        # self.transactions_df.show()

    def process_daily_batch(self, date: str) -> None:
        """Process orders for a specific date and update inventory, excluding out-of-stock items."""

        target_date = datetime.strptime(date, "%Y%m%d").date()
        items_df = self.transactions_df.filter(col("date") == target_date)

        # Prepare aliased inventory DataFrame
        inventory_alias = self.current_inventory.select(
            col("product_id").alias("inv_product_id"),
            col("stock").alias("inventory_stock"),
        )

        # Broadcast decision
        if self.current_inventory.count() < 100000:
            inventory_df = broadcast(inventory_alias)
        else:
            inventory_df = inventory_alias

        try:
            self.spark.sparkContext.setLocalProperty(
                "spark.scheduler.pool", "daily_processing"
            )

            # Join transactions with products
            order_line_items = (
                items_df.alias("trans")
                .join(
                    self.products_df.alias("prod"),
                    col("trans.product_id") == col("prod.product_id"),
                    "left",
                )
                .select(
                    col("trans.transaction_id"),
                    col("trans.product_id"),
                    col("trans.timestamp"),
                    col("trans.date"),
                    col("trans.customer_id"),
                    col("trans.qty"),
                    col("prod.sales_price"),
                    col("prod.cost_to_make"),
                )
            )

            # Join with inventory
            order_line_items = (
                order_line_items.alias("oli")
                .join(
                    inventory_df.alias("inv"),
                    col("oli.product_id") == col("inv.inv_product_id"),
                    "left",
                )
                .select("*", col("inv.inventory_stock"))
                .drop("inv_product_id")
            )

            # Inventory calculation windows
            window_cumulative = (
                Window.partitionBy("product_id")
                .orderBy("timestamp")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )

            window_lag = Window.partitionBy("product_id").orderBy("timestamp")

            # Fulfillment logic
            order_line_items = order_line_items.withColumn(
                "fulfilled_qty_initial", col("qty")
            )
            order_line_items = order_line_items.withColumn(
                "cumulative_fulfilled_initial",
                spark_sum("fulfilled_qty_initial").over(window_cumulative),
            )
            order_line_items = order_line_items.withColumn(
                "cumulative_fulfilled_prev",
                lag(col("cumulative_fulfilled_initial"), 1, 0).over(window_lag),
            )
            order_line_items = order_line_items.withColumn(
                "remaining_stock",
                col("inventory_stock") - col("cumulative_fulfilled_prev"),
            )
            order_line_items = order_line_items.withColumn(
                "fulfilled_qty",
                when(col("remaining_stock") >= col("qty"), col("qty")).otherwise(
                    greatest(col("remaining_stock"), lit(0))
                ),
            ).persist(StorageLevel.DISK_ONLY)

            order_line_items = order_line_items.withColumn(
                "cancelled_qty",
                when(
                    col("fulfilled_qty") < col("qty"), col("qty") - col("fulfilled_qty")
                ).otherwise(0),
            )

            daily_cancelled = (
                order_line_items.agg(
                    spark_sum(col("cancelled_qty")).alias("total_cancelled")
                ).first()["total_cancelled"]
                or 0
            )

            print(f"{daily_cancelled}, items cancelled on {target_date}.")
            self.total_cancelled_items += daily_cancelled

            # Update inventory
            total_fulfilled = order_line_items.groupBy("product_id").agg(
                spark_sum("fulfilled_qty").alias("total_fulfilled")
            )

            inventory_updates = (
                total_fulfilled.alias("tf")
                .join(self.current_inventory.alias("ci"), "product_id")
                .select(
                    "product_id",
                    (col("ci.stock") - col("tf.total_fulfilled")).alias("new_stock"),
                )
                .localCheckpoint()
            )

            self.current_inventory = (
                self.current_inventory.alias("ci")
                .join(inventory_updates.alias("iu"), "product_id", "left")
                .select(
                    "product_id",
                    coalesce(col("iu.new_stock"), col("ci.stock")).alias("stock"),
                )
                .checkpoint()
            )

            order_line_items = order_line_items.withColumn(
                "item_sales", col("fulfilled_qty") * col("sales_price")
            ).withColumn(
                "item_profit",
                col("item_sales") - (col("fulfilled_qty") * col("cost_to_make")),
            )

            # Order line items processing
            if not self.order_line_items_df:
                self.order_line_items_df = order_line_items.select(
                    col("transaction_id").alias("order_id"),
                    "product_id",
                    col("fulfilled_qty").alias("quantity"),
                    col("sales_price").alias("unit_price"),
                    col("item_sales").alias("line_total"),
                ).persist(StorageLevel.DISK_ONLY)
            else:
                self.order_line_items_df = self.order_line_items_df.union(
                    order_line_items.select(
                        col("transaction_id").alias("order_id"),
                        "product_id",
                        col("fulfilled_qty").alias("quantity"),
                        col("sales_price").alias("unit_price"),
                        col("item_sales").alias("line_total"),
                    )
                ).checkpoint(eager=True)

            # Orders aggregation
            if not self.orders_df:
                self.orders_df = (
                    order_line_items.groupBy("transaction_id")
                    .agg(
                        first("timestamp").alias("order_datetime"),
                        first("customer_id").alias("customer_id"),
                        spark_sum(col("item_sales")).alias("total_amount"),
                        count("timestamp").alias("num_items"),
                    )
                    .withColumnRenamed("transaction_id", "order_id")
                ).persist(StorageLevel.DISK_ONLY)
            else:
                self.orders_df = (
                    self.orders_df.union(
                        order_line_items.groupBy("transaction_id").agg(
                            first("timestamp").alias("order_datetime"),
                            first("customer_id").alias("customer_id"),
                            spark_sum(col("item_sales")).alias("total_amount"),
                            count("timestamp").alias("num_items"),
                        )
                    ).withColumnRenamed("transaction_id", "order_id")
                ).checkpoint(eager=True)

            self.calc_daily_summary(order_line_items)

        finally:
            self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)

        # Cleanup temporary DataFrames
        order_line_items.unpersist()
        self.order_line_items_df.unpersist()
        self.orders_df.unpersist()
        gc.collect()

    def calc_daily_summary(self, daily_df: DataFrame) -> None:
        """Calculate daily summary for fulfillable orders and append to daily_summary_df."""

        # Single-pass aggregation using selectExpr
        summary_df = (
            daily_df.selectExpr("date", "transaction_id", "item_sales", "item_profit")
            .groupBy("date")
            .agg(
                countDistinct("transaction_id").alias("num_orders"),
                spark_sum(col("item_sales"))
                .cast(DecimalType(10, 2))
                .alias("total_sales"),
                spark_sum(col("item_profit"))
                .cast(DecimalType(10, 2))
                .alias("total_profit"),
            )
        )

        summary_df.show()

        if not self.daily_summary_df:
            self.daily_summary_df = summary_df.cache()
        else:
            # Use checkpointing for long-term storage
            self.daily_summary_df = self.daily_summary_df.union(summary_df).checkpoint(
                eager=True
            )  # Truncate lineage

    def update_products_df(self) -> None:

        self.products_df = (
            self.products_df.alias("p")
            .join(self.current_inventory.alias("c"), "product_id", "left")
            .select(
                *[col(f"p.{c}") for c in self.products_df.columns if c != "stock"],
                coalesce(col("c.stock"), col("p.stock")).alias("stock"),
            )
            .orderBy("product_id")
        )

    def process_batches(self, date_range: list) -> None:
        """Process all days in the date range"""
        print("\nSTARTING BATCH PROCESSING")
        print("=" * 80)

        # Set checkpoint directory if not already set
        if not self.spark.sparkContext.getCheckpointDir():
            self.spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

        self.products_df = self.original_products_df

        # Initialize inventory if not already done
        if not self.inventory_initialized:
            self.current_inventory = self.products_df.select(
                "product_id", "product_name", "stock"
            )
            self.inventory_initialized = True

        # Process each day sequentially
        for date in date_range:
            self.process_daily_batch(date)
            # self.print_inventory_levels()  # Optional: print inventory after each day
            self.spark.catalog.clearCache()
            gc.collect()

        # Update and save products_df
        self.update_products_df()
        updated_to_save = self.products_df.select(
            "product_id", "product_name", "stock"
        ).withColumnRenamed("stock", "current_stock")
        self.save_to_csv(
            updated_to_save, self.__config["output_path"], "products_updated.csv"
        )

        # Save order_line_items
        self.save_to_csv(
            (
                self.order_line_items_df.orderBy("order_id", "product_id").withColumn(
                    "line_total", spark_round(col("line_total"), 2)
                )
            ),
            self.__config["output_path"],
            "order_line_items.csv",
        )

        # Save orders
        self.save_to_csv(
            (
                self.orders_df.orderBy("order_id")
                .withColumn(
                    "order_datetime",
                    date_format("order_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                )
                .withColumn("total_amount", spark_round(col("total_amount"), 2))
            ),
            self.__config["output_path"],
            "orders.csv",
        )

        # Save daily_summary
        self.save_to_csv(
            self.daily_summary_df, self.__config["output_path"], "daily_summary.csv"
        )

        # Finalize processing
        self.finalize_processing()

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)

    def finalize_processing(self) -> None:
        """Finalize processing and create summary"""
        print("\nPROCESSING COMPLETE")
        print("=" * 80)
        print(f"Total Cancelled Items: {self.total_cancelled_items}")

    # ------------------------------------------------------------------------------------------------
    # Try not to change the logic of the time series forecasting model
    # DO NOT change functions with prefix _
    # ------------------------------------------------------------------------------------------------
    def forecast_sales_and_profits(
        self, daily_summary_df: DataFrame, forecast_days: int = 1
    ) -> DataFrame:
        """
        Main forecasting function that coordinates the forecasting process
        """
        try:
            # Build model
            model_data = self.build_time_series_model(daily_summary_df)

            # Calculate accuracy metrics
            metrics = self.calculate_forecast_metrics(model_data)

            # Generate forecasts
            forecast_df = self.make_forecasts(model_data, forecast_days)

            return forecast_df

        except Exception as e:
            print(
                f"Error in forecast_sales_and_profits: {str(e)}, please check the data"
            )
            return None

    def print_inventory_levels(self) -> None:
        """Print current inventory levels for all products"""
        print("\nCURRENT INVENTORY LEVELS")
        print("-" * 40)

        inventory_data = self.current_inventory.orderBy("product_id").collect()
        for row in inventory_data:
            print(
                f"• {row['product_name']:<30} (ID: {row['product_id']:>3}): {row['stock']:>4} units"
            )
        print("-" * 40)

    def build_time_series_model(self, daily_summary_df: DataFrame) -> dict:
        """Build Prophet models for sales and profits"""
        print("\n" + "=" * 80)
        print("TIME SERIES MODEL CONSTRUCTION")
        print("-" * 80)

        model_data = self._prepare_time_series_data(daily_summary_df)
        return self._fit_forecasting_models(model_data)

    def calculate_forecast_metrics(self, model_data: dict) -> dict:
        """Calculate forecast accuracy metrics for both models"""
        print("\nCalculating forecast accuracy metrics...")

        # Get metrics from each model
        sales_metrics = model_data["sales_model"].get_metrics()
        profit_metrics = model_data["profit_model"].get_metrics()

        metrics = {
            "sales_mae": sales_metrics["mae"],
            "sales_mse": sales_metrics["mse"],
            "profit_mae": profit_metrics["mae"],
            "profit_mse": profit_metrics["mse"],
        }

        # Print metrics and model types
        print("\nForecast Error Metrics:")
        print(f"Sales Model Type: {sales_metrics['model_type']}")
        print(f"Sales MAE: ${metrics['sales_mae']:.2f}")
        print(f"Sales MSE: ${metrics['sales_mse']:.2f}")
        print(f"Profit Model Type: {profit_metrics['model_type']}")
        print(f"Profit MAE: ${metrics['profit_mae']:.2f}")
        print(f"Profit MSE: ${metrics['profit_mse']:.2f}")

        return metrics

    def make_forecasts(self, model_data: dict, forecast_days: int = 7) -> DataFrame:
        """Generate forecasts using Prophet models"""
        print(f"\nGenerating {forecast_days}-day forecast...")

        forecasts = self._generate_model_forecasts(model_data, forecast_days)
        forecast_dates = self._generate_forecast_dates(
            model_data["training_data"]["dates"][-1], forecast_days
        )

        forecast_df = (
            self._create_forecast_dataframe(forecast_dates, forecasts)
            .withColumn("forecasted_sales", spark_round(col("forecasted_sales"), 2))
            .withColumn("forecasted_profit", spark_round(col("forecasted_profit"), 2))
        )
        return forecast_df

    def _prepare_time_series_data(self, daily_summary_df: DataFrame) -> dict:
        """Prepare data for time series modeling"""
        data = (
            daily_summary_df.select("date", "total_sales", "total_profit")
            .orderBy("date")
            .collect()
        )

        dates = np.array([row["date"] for row in data])
        sales_series = np.array([float(row["total_sales"]) for row in data])
        profit_series = np.array([float(row["total_profit"]) for row in data])

        self._print_dataset_info(dates, sales_series, profit_series)

        return {"dates": dates, "sales": sales_series, "profits": profit_series}

    def _print_dataset_info(
        self, dates: np.ndarray, sales: np.ndarray, profits: np.ndarray
    ) -> None:
        """Print time series dataset information"""
        print("Dataset Information:")
        print(f"• Time Period:          {dates[0]} to {dates[-1]}")
        print(f"• Number of Data Points: {len(dates)}")
        print(f"• Average Daily Sales:   ${np.mean(sales):.2f}")
        print(f"• Average Daily Profit:  ${np.mean(profits):.2f}")

    def _fit_forecasting_models(self, data: dict) -> dict:
        """Fit Prophet models to the prepared data"""
        print("\nFitting Models...")
        sales_forecaster = ProphetForecaster()
        profit_forecaster = ProphetForecaster()

        sales_forecaster.fit(data["sales"])
        profit_forecaster.fit(data["profits"])
        print("Model fitting completed successfully")
        print("=" * 80)

        return {
            "sales_model": sales_forecaster,
            "profit_model": profit_forecaster,
            "training_data": data,
        }

    def _generate_model_forecasts(self, model_data: dict, forecast_days: int) -> dict:
        """Generate forecasts from both models"""
        return {
            "sales": model_data["sales_model"].predict(forecast_days),
            "profits": model_data["profit_model"].predict(forecast_days),
        }

    def _generate_forecast_dates(self, last_date: datetime, forecast_days: int) -> list:
        """Generate dates for the forecast period"""
        return [last_date + timedelta(days=i + 1) for i in range(forecast_days)]

    def _create_forecast_dataframe(self, dates: list, forecasts: dict) -> DataFrame:
        """Create Spark DataFrame from forecast data"""
        forecast_rows = [
            (date, float(sales), float(profits))
            for date, sales, profits in zip(
                dates, forecasts["sales"], forecasts["profits"]
            )
        ]

        return self.spark.createDataFrame(
            forecast_rows, ["date", "forecasted_sales", "forecasted_profit"]
        )
