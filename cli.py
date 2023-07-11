
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

import os
import time

# UDF
from creds import read_cred_from_file
from get_user_choice import get_user_choice
from spark_queries import transactions_by_zip_month_year,\
                          transaction_total_and_no_by_type,\
                          transaction_total_and_no_by_branch_on_state,\
                          show_account_details,\
                          update_dataframe,\
                          display_monthly_bill,\
                          transaction_between_dates

#--------------------------------------------------------------------------------------------#

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create a SparkSession with the given app name.

    Args:
        app_name (str): Name of the Spark application.

    Returns:
        SparkSession: Initialized SparkSession object.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    return spark


def read_table_from_mysql(spark: SparkSession, url: str, table: str, properties: dict) -> DataFrame:
    """
    Read a table from MySQL database into a Spark DataFrame.

    Args:
        spark (SparkSession): SparkSession object.
        url (str): JDBC URL of the MySQL database.
        table (str): Name of the table to read.
        properties (dict): Connection properties for MySQL database.

    Returns:
        DataFrame: Spark DataFrame containing the table data.
    """
    try:
        df = spark.read.jdbc(url=url, table=table, properties=properties)
        return df
    except AnalysisException as e:
        raise ValueError(f"Error reading table '{table}' from MySQL: {str(e)}")


# Spark configuration
spark = create_spark_session('Query MySQL database: creditcard_capstone')

# Access the MySQL credentials
username, password = read_cred_from_file()

# MySQL connection properties
mysql_database_name = 'creditcard_capstone'
mysql_url = f'jdbc:mysql://localhost:3306/{mysql_database_name}'
mysql_properties = {
    'user': username,
    'password': password
}

# Read tables from MySQL
try:
    customer_df = read_table_from_mysql(spark, mysql_url, 'cdw_sapp_customer', mysql_properties)
    credit_df = read_table_from_mysql(spark, mysql_url, 'cdw_sapp_credit_card', mysql_properties)
    branch_df = read_table_from_mysql(spark, mysql_url, 'cdw_sapp_branch', mysql_properties)

except Exception as e:
    print(f"error reading data from mysql database {e}")


def wait_for_user():
    input('\nPress  any button to return to main menu')

def main():
    os.system('clear')
    choice = get_user_choice()

    while choice.lower() != 'q':
        try:
            if choice == '1':
                transactions_by_zip_month_year(customer_df, credit_df)
            elif choice == '2':
                transaction_total_and_no_by_type(credit_df)
            elif choice == '3':
                transaction_total_and_no_by_branch_on_state(branch_df, credit_df)
            elif choice == '4':
                show_account_details(customer_df)
            elif choice == '5':
                update_dataframe(customer_df)
                print('Data sucessfully updated.\n')
            elif choice == '6':
                display_monthly_bill(credit_df)
                pass
            elif choice == '7':
                transaction_between_dates(credit_df)
                pass
            else:
                print("\nInvalid choice. Please try again.")
            
            wait_for_user()
            
            choice = get_user_choice()  # Prompt for next choice
        except Exception as e:
            print(f"An error occurred: {e}")
            choice = get_user_choice()  # Prompt for next choice
    
    print("\n Quitting... ")
    time.sleep(1)
    os.system('clear')

if __name__ == '__main__':
    main()
