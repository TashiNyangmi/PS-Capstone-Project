"""
Credit Card Analysis Application

This Python script contains functions for performing various operations on credit card data and customer information. It utilizes PySpark and SQLAlchemy for data processing and MySQL for database operations. The functions provide functionality to query and analyze credit card transactions, retrieve and update customer details, and generate monthly bills.

Author: Tashi Tsering Gurung
Email: hseb.tashi@gmail.com
Date: 07/20/2023

Requirements:
- PySpark
- SQLAlchemy
- MySQL Connector/Python

Usage:
1. Ensure that the necessary dependencies are installed.
2. Configure the MySQL database connection parameters in the create_sql_connection function.
3. Provide the appropriate input data and execute the desired function calls.
4. Follow the instructions and prompts provided by the application to interact with the data.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import count, sum, col, when, to_date
from sqlalchemy import create_engine, text

import mysql.connector
from mysql.connector import Error

from datetime import datetime

def get_zipcode() -> str:
    """
    Prompts the user to enter a valid 5-digit zipcode.

    Returns:
        str: The valid 5-digit zipcode entered by the user.
    """
    def validate_zipcode(zipcode: str) -> None:
        """
        Validates the format of the given zipcode.

        Args:
            zipcode (str): The zipcode to validate.

        Raises:
            ValueError: If the zipcode is not a 5-digit numeric value.
        """
        if len(zipcode) != 5 or not zipcode.isdigit():
            raise ValueError("Invalid zipcode. Please enter a 5-digit numeric value.")

    while True:
        try:
            zipcode = input("Zipcode: ")
            validate_zipcode(zipcode)
            break
        except ValueError as e:
            print(str(e))

    return zipcode


def get_month() -> int:
    """
    Prompts the user to enter a valid month.

    Returns:
        int: The valid month entered by the user (numeric value between 1 and 12).
    """
    def validate_month(month: int) -> None:
        """
        Validates the given month.

        Args:
            month (int): The month to validate.

        Raises:
            ValueError: If the month is not a numeric value between 1 and 12.
        """
        if not 1 <= month <= 12:
            raise ValueError("Invalid month. Please enter a numeric value between 1 and 12.")

    while True:
        try:
            month = int(input("Month: "))
            validate_month(month)
            break
        except ValueError as e:
            print(str(e))

    return month


def get_year() -> int:
    """
    Prompts the user to enter a valid year.

    Returns:
        int: The valid year entered by the user (4-digit numeric value).
    """
    def validate_year(year: int) -> None:
        """
        Validates the given year.

        Args:
            year (int): The year to validate.

        Raises:
            ValueError: If the year is not a 4-digit numeric value.
        """
        if len(str(year)) != 4 or not str(year).isdigit():
            raise ValueError("Invalid year. Please enter a 4-digit numeric value.")

    while True:
        try:
            year = int(input("Year: "))
            validate_year(year)
            break
        except ValueError as e:
            print(str(e))

    return year


def choose_one_from(available_options: list, msg: str = "Available Options") -> str:
    """
    Prompts the user to choose one option from the given list of available options.

    Args:
        available_options (list): The list of available options to choose from.
        msg (str, optional): The message to display when prompting the user for input. Defaults to "Available Options".

    Returns:
        str: The selected option entered by the user.

    Raises:
        ValueError: If the available_options list is empty.
    """
    if not available_options:
        raise ValueError("No options provided.")

    while True:
        selected_option = input(f"{msg} ({', '.join(available_options)}): ")
        if selected_option in available_options:
            break
        print(f"Invalid option. Please enter a valid option from the given list: {', '.join(available_options)}")

    return selected_option


def get_valid_input(df: DataFrame, column_name: str) -> str:
    """
    Prompts the user to enter a valid input from the distinct options available in the specified column of a DataFrame.

    Args:
        df (DataFrame): The DataFrame containing the data.
        column_name (str): The name of the column to get valid input from.

    Returns:
        str: The valid input entered by the user.

    Raises:
        ValueError: If the DataFrame or column name is not provided.
    """
    valid_input_list = distinct_options(df=df, column_name=column_name)
    valid_options = ", ".join(valid_input_list)

    while True:
        input_value = input(f"{column_name} ({valid_options}): ")
        if input_value in valid_input_list:
            break
        print(f"Invalid {column_name}. Please enter a valid input from the given options: {valid_options}.")

    return input_value


def get_valid_16_digit_input() -> str:
    """
    Prompts the user to enter a valid 16-digit number.

    Returns:
        str: The valid 16-digit number entered by the user.
    """
    while True:
        input_value = input("Enter a 16-digit number: ")
        if input_value.isdigit() and len(input_value) == 16:
            break
        print("Invalid input. Please enter a 16-digit number.")

    return input_value


# Module: Transaction Queries
def transactions_by_zip_month_year(customer_df, credit_df) -> None:
    """
    Displays transactions made by customers living in a given zip code for a given month and year,
    ordered by day in ascending order.

    Args:
        customer_df (DataFrame): DataFrame containing customer data.
        credit_df (DataFrame): DataFrame containing credit transaction data.

    Returns:
        None
    """
    try:
        zipcode = get_zipcode()
        month = get_month()
        year = get_year()
        
        filters = (customer_df.CUST_ZIP == zipcode) & (credit_df.MONTH == month) & (credit_df.YEAR == year)

        result_df = credit_df.join(customer_df, credit_df.CUST_SSN == customer_df.SSN, 'left') \
                             .where(filters) \
                             .orderBy(credit_df.DAY)

        result_df.show(truncate=False, n=1000)
    except Exception as e:
        print(f"Error: {str(e)}")


def distinct_options(df: DataFrame, column_name: str) -> list[str]:
    """
    Get the distinct options from a specific column in the DataFrame.

    Args:
        df (DataFrame): The DataFrame containing the data.
        column_name (str): The name of the column to fetch distinct options from.

    Returns:
        list[str]: The list of distinct options from the specified column.
    """
    distinct_options = df.select(column_name).distinct().rdd.flatMap(lambda x: x).collect()
    return distinct_options


def transaction_total_and_no_by_type(credit_df) -> None:
    """
    Displays the number and total values of transactions for a given type.

    Args:
        credit_df (DataFrame): DataFrame containing credit transaction data.

    Returns:
        None

    Raises:
        ValueError: If the input transaction type is not valid.
    """
    try:
        transaction_type = get_valid_input(credit_df, column_name='TRANSACTION_TYPE') 

        result_df = credit_df.filter(credit_df.TRANSACTION_TYPE == transaction_type) \
                             .select(count('TRANSACTION_ID').alias('Total no of transactions'), \
                                     sum('TRANSACTION_VALUE').alias('Sum of Transaction Values'))

        result_df.show(truncate=False, n=1000)
    except Exception as e:
        print(f"Error: {str(e)}")


def transaction_total_and_no_by_branch_on_state(branch_df, credit_df) -> None:
    """
    Displays the total number and total values of transactions for branches in a given state.

    Args:
        branch_df (DataFrame): DataFrame containing branch data.
        credit_df (DataFrame): DataFrame containing credit transaction data.

    Returns:
        None
    """
    try:
        state = get_valid_input(branch_df, column_name='BRANCH_STATE')

        result_df = credit_df.join(branch_df, credit_df.BRANCH_CODE == branch_df.BRANCH_CODE, 'left') \
                             .where(branch_df.BRANCH_STATE == 'MN') \
                             .groupBy(branch_df.BRANCH_CODE) \
                             .agg(count(credit_df.TRANSACTION_ID).alias('Transaction_Count'), \
                                  sum(credit_df.TRANSACTION_VALUE).alias('Transaction_Sum'))

        result_df.show(truncate=False, n=1000)
    except Exception as e:
        print(f"Error: {str(e)}")


def show_account_details(customer_df) -> None:
    """
    Retrieves and displays account details based on user input.

    Args:
        customer_df (DataFrame): DataFrame containing customer data.

    Returns:
        None
    """
    try:
        valid_column_names = customer_df.columns

        filter_column = choose_one_from(valid_column_names)
        filter_value = get_valid_input(df=customer_df, column_name=filter_column)

        result_df = customer_df.filter(customer_df[filter_column] == filter_value)
        result_df.show(truncate=False, n=1000)
    except Exception as e:
        print(f"Error: {str(e)}")


def create_sql_connection():
    """
    Creates a MySQL database connection.

    Returns:
        mysql.connector.connection.MySQLConnection: The MySQL connection object.
    """
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='creditcard_capstone',
            user='root',
            password='password'
        )
        if connection.is_connected():
            print('Connected to MySQL database.')
            return connection

    except Error as e:
        print(f'Error connecting to MySQL database: {e}')
        return None



def update_dataframe(customer_df) -> None:
    """
    Update values in a PySpark DataFrame based on a filter condition.

    Returns:
        None.
    """
    try:
        valid_column_names = customer_df.columns

        filter_column = choose_one_from(valid_column_names, msg='Select Column to filter with')
        filter_value = get_valid_input(df=customer_df, column_name=filter_column)

        column_to_update = choose_one_from(valid_column_names, msg='Column to update')
        new_value = input("Enter new value:")  # can implement appropriate input validation here

        # update spark dataframe
        customer_df = customer_df.withColumn(
            column_to_update,
            when(col(filter_column) == filter_value, new_value).otherwise(col(column_to_update)))

        # update SQL table
        # SQL update statement
        sql_update = text(f"UPDATE CDW_SAPP_CUSTOMER SET {column_to_update} = '{new_value}' WHERE {filter_column} = '{filter_value}'")

        engine = create_sql_connection()
        # Execute the update statement
        with engine.begin() as connection:
            connection.execute(sql_update)
    except Exception as e:
        print(f"Error: {str(e)}")

def generate_monthly_bill(credit_df) -> DataFrame:
    """
    Generates a monthly bill based on user input.

    Returns:
        DataFrame: The resulting DataFrame containing the selected columns for the specified credit card number, month, and year.
    """
    try:
        print("Please enter the credit card number.")
        input_cc_no = get_valid_16_digit_input()
        input_year = get_year()
        input_month = get_month()

        result_df = credit_df.filter((credit_df['CREDIT_CARD_NO'] == input_cc_no) &
                                     (credit_df['MONTH'] == input_month) &
                                     (credit_df['YEAR'] == input_year))

        return result_df.select(['TRANSACTION_ID', 'DAY', 'MONTH', 'YEAR', 'TRANSACTION_TYPE', 'TRANSACTION_VALUE'])
    except Exception as e:
        print(f"Error: {str(e)}")

def print_transaction_summary(df: DataFrame) -> None:
    """
    Prints a summary of the transactions in the provided DataFrame.

    Args:
        df (DataFrame): The DataFrame containing the transactions.

    Returns:
        None
    """
    try:
        total_and_count = df.agg({'TRANSACTION_VALUE': 'SUM', 'TRANSACTION_ID': 'COUNT'}).first()
        total = total_and_count['sum(TRANSACTION_VALUE)']
        count = total_and_count['count(TRANSACTION_ID)']

        print(f'Summary: There were {count} transactions totaling $ {total}')
    except Exception as e:
        print(f"Error: {str(e)}")

def display_monthly_bill(credit_df) -> None:
    """
    Displays the monthly bill and transaction summary.

    Returns:
        None
    """
    try:
        result_df = generate_monthly_bill(credit_df)
        result_df.show(truncate=False, n=1000)
        print_transaction_summary(result_df)
    except Exception as e:
        print(f"Error: {str(e)}")


def get_date_input() -> str:
    """
    Prompts the user to enter a valid date in the format YYYY-MM-DD.

    Returns:
        str: The valid date entered by the user.

    Raises:
        ValueError: If the input date is not in the correct format.
    """
    while True:
        date_str = input("Enter a date (YYYY-MM-DD): ")
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return date_str
        except ValueError:
            print("Invalid date format. Please enter a date in the format YYYY-MM-DD.")


def transaction_between_dates(credit_df) -> None:
    """
    Displays transactions between two specified dates and prints a transaction summary.

    Returns:
        None
    """
    try:
        print('Start Date')
        start_date = get_date_input()

        print('End Date')
        end_date = get_date_input()

        print("Please enter the credit card number.")
        result_df = credit_df.filter((to_date(col('TIMEID'), 'yyyyMMdd') >= start_date) &
                                     (to_date(col('TIMEID'), 'yyyyMMdd') <= end_date) &
                                     (col('CREDIT_CARD_NO') == get_valid_16_digit_input()))

        result_df.show(truncate=False, n=1000)
        print_transaction_summary(result_df)
    except Exception as e:
        print(f"Error: {str(e)}")

