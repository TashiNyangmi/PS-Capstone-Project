#!/usr/bin/env python
# coding: utf-8

"""
This script retrieves loan data from an API, processes it with Spark, and writes it to a MySQL database.
"""

import requests
from json_to_mysql import spark_to_sql
from pyspark.sql import SparkSession
from creds import read_cred_from_file


def retrieve_loan_data(url):
    """
    Retrieves loan data from the given URL.

    Args:
        url (str): The URL to retrieve loan data from.

    Returns:
        list: The loan data as a list of dictionaries.
    """
    response = requests.get(url)
    if response.status_code == 200:
        loan_data = response.json()
        print("Loan data retrieved successfully!")
        print("Number of loan applications:", len(loan_data))
        return loan_data
    else:
        print("Failed to retrieve loan data. Status code:", response.status_code)
        return []


# API URL
url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"

# Retrieve loan data
loan_data = retrieve_loan_data(url)

# Create a Spark session
spark = SparkSession.builder.appName("APItoMySQL").getOrCreate()

# Convert loan data to Spark DataFrame
loan_df = spark.createDataFrame(loan_data)

# MySQL database details
db_name = 'creditcard_capstone'
table_name = 'CDW_SAPP_loan_application'
url = f'jdbc:mysql://localhost:3306/{db_name}'

# Read database credentials from file
username, password = read_cred_from_file()
properties = {
    'user': username,
    'password': password,
    'driver': 'com.mysql.jdbc.Driver'
}

# Create database if not exists and write loan data to MySQL
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
spark.sql(f"USE {db_name}")
spark_to_sql(loan_df, table_name=table_name, url=url, properties=properties)

# Stop the Spark session
spark.stop()


"""
Author: Tashi T. Gurung
Contact: hseb.tashi@gmail.com
"""