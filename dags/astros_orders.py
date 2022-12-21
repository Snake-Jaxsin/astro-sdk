from datetime import datetime

from airflow.models import DAG
from pandas import DataFrame

#allows interact with python & sql
from astro import sql as aql

#allows to interact with files
from astro.files import File

#allows to interact with tables
from astro.sql.table import Table

# Env Var
S3_FILE_PATH = "s3://j-astrosdk"
S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_ORDERS = "orders_table"
SNOWFLAKE_FILTER_ORDER = "filtered_table"
SNOWFLAKE_JOINED = "joined_table"
SNOWFLAKE_CUSTOMERS = "customers_table"
SNOWFLAKE_REPORTING = "reporting_table"

# Filtering tables.
@aql.transform
def filter_orders(input_table: Table):
    return ("SELECT * FROM {{input_table}} WHERE amount > 150")

# Joining Tables.
@aql.transform
def join_orders_customers(filtered_orders_table: Table, customers_table: Table):
    return """SELECT c.customer_id, customer_name, order_id, purchase_date, amount, type
    FROM {{filtered_orders_table}} f JOIN {{customers_table}} c 
    ON f.customer_id = c.customer_id"""

@aql.dataframe
def transform_dataframe(df: DataFrame):
    purchase_dates = df.loc[:, "purchase_dates"]
    print("purchased_dates:")
    return purchase_dates

# Dag.
with DAG(dag_id='astro_orders', start_date=datetime(2022, 1, 1),schedule='@daily', catchup=False):
    orders_data = aql.load_file(
        input_file=File(
            path=S3_FILE_PATH + "/orders_data_header.csv", conn_id=S3_CONN_ID
        ),
        output_table=Table(conn_id=SNOWFLAKE_CONN_ID)
    )

    customers_table = Table(
        name=SNOWFLAKE_CUSTOMERS,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # Data Merge.
    joined_data = join_orders_customers(filter_orders(orders_data),customers_table)

    reporting_table = aql.merge(
        target_table=Table(
            name=SNOWFLAKE_REPORTING,
            conn_id=SNOWFLAKE_CONN_ID,
        ),
        source_table=joined_data,
        target_conflict_columns=["order_id"],
        columns=["customer_id", "customer_name"],
        if_conflicts="update",
    )

    purchase_dates = transform_dataframe(reporting_table)

    purchase_dates >> aql.cleanup()



