# Batch Processing System with Sales Forecasting for Tiger's Candy

## Description
In this project, we implement batch processing logic to process raw order transactions at the end of each day. This process includes validating transaction details and verifying inventory levels to ensure successful order shipment. Then, the aggregated daily sales and profit numbers are put into a time series forecasting model to predict future sales and profits.

Dataset:
- Transactions: Each day has a transaction collection in MongoDB, where each collection contains information about each order from a particular customer.
- Products: The products table in MySQL contains information about the products avaialble and the current available stock.
- Customers: The customers table in MySQl contains information about each unique customer to Tiger's candy.

## Prerequisites
- Python 3.12
- OpenJDK 17
- MySQL 9.0.1
- MongoDB Community Edition 7.0


## Installation
### Clone git repository:
```
git clone https://github.com/rahulkfernandes/Batch-Processing-System-with-Sales-Forecasting-for-Tigers-Candy.git
```

### Install dependencies:
```
pip install pyspark numpy dotenv scikit-learn prophet
```

### Required Connectors
- Download MySQL Connector/J (9.1.0) and save the jar file in an appropriate directory and add the path do your .env file

### Setting Up Environment Variables

1. Create your local environment file:
   ```bash
   cp .env.example .env
   ```

2. Update the `.env` file with your database credentials and paths

## Usage
### To run the batch processing
```
python src/main.py
```

### To run and monitor the workflow through Apache Airflow
To add DAG to airflow:
Copy the `candy_store_dag.py` file and paste it in the `<Path-to-Airflow-Home-Directory>/dags/` directory. Usually it is `~/airflow/dags/`.
Then, add the path to your project root folder here, in the `candy_store_dag.py` file:
```
# Airflow-specific path configuration
PROJECT_PATH = os.environ.get(
    "PROJECT_PATH", "<Project-Root-Path>"
)
SRC_PATH = os.path.join(PROJECT_PATH, "src")
```

To run the Airflow webserver:
```
airflow webserver --port 8080
```

In another terminal window: (To run the scheduler)
```
airflow scheduler
```

You can access the Airflow UI by going to http://localhost:8080 in your web browser.
