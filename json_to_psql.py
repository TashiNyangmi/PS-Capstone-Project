#!/usr/bin/env python
# coding: utf-8

# In[5]:


from pyspark.sql import SparkSession
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import StringType
from pyspark.sql import Row

from pyspark.sql.functions import col, \
                                  lower, initcap, \
                                  concat, concat_ws, lit, substring, format_string

from typing import List, Optional, Union

import mysql.connector


# In[6]:


ss = SparkSession.builder.master('local[2]').appName('format_json').getOrCreate()


# ---

# UDF

# In[7]:


def apply_format_phone_no(column: Union[str, Column]) -> Column:
    """
    Applies phone number formatting to the given Spark Column.

    Args:
        column (Union[str, Column]): The Spark Column or column name representing the phone number.

    Returns:
        Column: A new Spark Column with the phone number formatted as '(XXX)-XXX-XXXX'.

    Raises:
        ValueError: If the provided column is not of type str or Column.

    Example:
        df = df.withColumn('formatted_phone', apply_format_phone_no(df['phone']))

    """

    # Parameter Validation
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


# In[2]:


def spark_to_sql(df: DataFrame, url: str, table_name: str = 'table1',  properties: Optional[dict] = None) -> None:
    """
    Writes the given Spark DataFrame to a SQL database table using JDBC.

    Args:
        df (DataFrame): The Spark DataFrame to be written to the SQL database.
        table_name (str, optional): The name of the table in the SQL database. Default is 'table1'.
        url (str, optional): The JDBC URL of the SQL database.
        properties (dict, optional): Additional properties for the JDBC connection. Default is None.

    Raises:
        ValueError: If the input DataFrame or JDBC URL is not provided.

    Example:
        spark_to_sql(df, table_name='my_table', url='jdbc:mysql://localhost:3306/my_database', properties={'user': 'my_user', 'password': 'my_password'})

    """
    if not df:
        raise ValueError("Input DataFrame 'df' is required.")
    
    if not url:
        raise ValueError("JDBC URL 'url' is required.")
    
    if properties is None:
        properties = {}

    try:
        df.write.jdbc(url=url, table=table_name, mode='overwrite', properties=properties)
    except Exception as e:
        raise Exception(f"Error occurred while writing data to the database: {str(e)}")


# ---

# 1. Dataset: Custmer

# In[12]:


customer_json = 'data/raw_data/cdw_sapp_custmer.json'

customer_df = ss.read\
            .option("multiline", False)\
            .json(customer_json)


# In[13]:


customer_df = (customer_df
    .withColumn('FIRST_NAME', initcap('FIRST_NAME'))
    .withColumn('MIDDLE_NAME', lower('MIDDLE_NAME'))
    .withColumn('LAST_NAME', initcap('LAST_NAME'))
    .withColumn('FULL_STREET_ADDRESS', concat_ws(',', 'STREET_NAME', 'APT_NO'))
    .withColumn('CUST_PHONE', apply_format_phone_no('CUST_PHONE'))
)


# ---

# 2. Dataset: Branch

# In[14]:


branch_json = 'data/raw_data/cdw_sapp_branch.json'

branch_df = ss.read\
            .option("multiline", False)\
            .json(branch_json)


# In[15]:


branch_df = branch_df.fillna(99999, subset = 'BRANCH_ZIP')
branch_df = branch_df.withColumn('BRANCH_PHONE', apply_format_phone_no('BRANCH_PHONE'))


# ---

# 3. Dataset: Credit

# In[16]:


credit_json = 'data/raw_data/cdw_sapp_credit.json'
credit_df = ss.read\
            .option('multiline', False)\
            .json(credit_json)


# In[17]:


credit_df = credit_df.withColumn('TIMEID', concat_ws('', 
                                                     'YEAR', 
                                                     format_string('%02d', 'MONTH'), 
                                                     format_string('%02d','DAY')))


# ---

# ### SQL

# In[18]:


# Connect to the MySQL server
conn = mysql.connector.connect(
    host='localhost',
    port='3306',
    user='root',
    password='password'
)


# In[19]:


db_name = 'creditcard_capstone'
cursor = conn.cursor()
cursor.execute(f'DROP DATABASE IF EXISTS {db_name};')
cursor.execute(f'CREATE DATABASE {db_name};')


# In[20]:


url = f'jdbc:mysql://localhost:3306/{db_name}'
properties = {
    'user': 'root',
    'password': 'password',
    'driver': 'com.mysql.jdbc.Driver'
}


# In[21]:


spark_to_sql(customer_df, table_name='customer', url = url, properties = properties)
spark_to_sql(branch_df, table_name='branch', url = url, properties = properties)
spark_to_sql(credit_df, table_name='credit', url = url, properties = properties)


# ---

# ## END
