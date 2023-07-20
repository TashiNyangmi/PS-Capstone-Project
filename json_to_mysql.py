#!/usr/bin/env python
# coding: utf-8

"""
This script performs data transformations on various datasets and writes them to a MySQL database.
"""

from pyspark.sql import SparkSession
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import StringType
from pyspark.sql import Row
from pyspark.sql.functions import col, lower, initcap, concat, concat_ws, lit, substring, format_string
from typing import List, Optional, Union
import mysql.connector
from creds import read_cred_from_file


def apply_format_phone_no(column: Union[str, Column]) -> Column:
    """
    Applies phone number formatting to the given Spark Column.

    Args:
        column (Union[str, Column]): The Spark Column or column name representing the phone number.

    Returns:
        Column: A new Spark Column with the phone number formatted as '(XXX)-XXX-XXXX'.

    Raises:
        ValueError: If the provided column is not of type str or Column.
    """
    if isinstance(column, str):
        column = col(column)
    elif not isinstance(column, Column):
        raise ValueError("The column parameter should be of type str or Column.")

    return (concat(lit('('),
                   substring(column, 1, 3),
                   lit(')-'),
                   substring(column, 4, 3),
                   lit('-'),
                   substring(column, 7, 4)
                   )
            )


def spark_to_sql(df: DataFrame, url: str, table_name: str = 'table1', properties: Optional[dict] = None) -> None:
    """
    Writes the given Spark DataFrame to a SQL database table using JDBC.

    Args:
        df (DataFrame): The Spark DataFrame to be written to the SQL database.
        table_name (str, optional): The name of the table in the SQL database. Default is 'table1'.
        url (str, optional): The JDBC URL of the SQL database.
        properties (dict, optional): Additional properties for the JDBC connection. Default is None.

    Raises:
        ValueError: If the input DataFrame or JDBC URL is not provided.
    """
    if not df:
        raise ValueError("Input DataFrame 'df' is required.")
    if not url:
        raise ValueError("JDBC URL 'url' is required.")
    if properties is None:
        properties = {}

    try:
        df.write.jdbc(url=url, table=table_name, mode='overwrite', properties=properties)
        print(f'Table {table_name} was successfully written to the database')
    except Exception as e:
        raise Exception(f"Error occurred while writing data to the database: {str(e)}")



# Create a Spark session
spark = SparkSession.builder.master('local[2]').appName('format_json').getOrCreate()


# ---


# 1. Dataset: Customer
customer_json = 'data/raw_data/cdw_sapp_custmer.json'
customer_df = spark.read.option("multiline", False).json(customer_json)

# Apply transformations to the Customer dataset
customer_df = (customer_df
    .withColumn('FIRST_NAME', initcap('FIRST_NAME'))
    .withColumn('MIDDLE_NAME', lower('MIDDLE_NAME'))
    .withColumn('LAST_NAME', initcap('LAST_NAME'))
    .withColumn('FULL_STREET_ADDRESS', concat_ws(',', 'STREET_NAME', 'APT_NO'))
    .withColumn('CUST_PHONE', apply_format_phone_no('CUST_PHONE'))
)


# ---


# 2. Dataset: Branch
branch_json = 'data/raw_data/cdw_sapp_branch.json'
branch_df = spark.read.option("multiline", False).json(branch_json)

# Apply transformations to the Branch dataset
branch_df = branch_df.fillna(99999, subset='BRANCH_ZIP')
branch_df = branch_df.withColumn('BRANCH_PHONE', apply_format_phone_no('BRANCH_PHONE'))


# ---


# 3. Dataset: Credit
credit_json = 'data/raw_data/cdw_sapp_credit.json'
credit_df = spark.read.option('multiline', False).json(credit_json)

# Apply transformations to the Credit dataset
credit_df = credit_df.withColumn('TIMEID', concat_ws('', 'YEAR', format_string('%02d', 'MONTH'), format_string('%02d', 'DAY')))


# ---


# Connect to the MySQL server
username, password = read_cred_from_file()
conn = mysql.connector.connect(
    host='localhost',
    port='3306',
    user=username,
    password=password
)

# Create the database if it doesn't exist
db_name = 'creditcard_capstone'
cursor = conn.cursor()
cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name};')
url = f'jdbc:mysql://localhost:3306/{db_name}'

properties = {
    'user': 'root',
    'password': 'password',
    'driver': 'com.mysql.jdbc.Driver'
}


# ---


# Function/Method Comments
def main():
    """
    Main entry point of the script.
    """
    # Write the datasets to SQL tables
    spark_to_sql(customer_df, table_name='CDW_SAPP_CUSTOMER', url=url, properties=properties)
    spark_to_sql(branch_df, table_name='CDW_SAPP_BRANCH', url=url, properties=properties)
    spark_to_sql(credit_df, table_name='CDW_SAPP_CREDIT_CARD', url=url, properties=properties)

    spark.stop()


# ---


# Dependencies
"""
Dependencies:
- pyspark
- mysql-connector-python
"""


# ---


# Version history
"""
Version History:
- v1.0 (2023-07-19): Initial version of the script.
"""


# ---


# Author and contact information
"""
Author: Tashi Gurung
Contact: hseb.tashi@gmail.com
"""


# Execute the main function
if __name__ == '__main__':
    main()

# ---


# Usage examples
"""
Usage Examples:
- Ensure the necessary dependencies are installed: pyspark, mysql-connector-python.
- Place the required credentials file in the appropriate location.
- Execute the script by running the 'main()' function.
"""
