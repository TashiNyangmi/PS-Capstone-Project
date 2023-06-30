from pyspark.sql import DataFrame

from pyspark.sql.types import DateType
from pyspark.sql.functions import count, sum, col, when, to_date
from sqlalchemy import create_engine, text

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


# In[11]:


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


# In[12]:


def transactions_by_zip_month_year(customer_df, credit_df) -> None:
    """
    Displays transactions made by customers living in a given zip code for a given month and year,
    ordered by day in ascending order.
    """
    filters = (customer_df.CUST_ZIP == get_zipcode()) & \
              (credit_df.MONTH == get_month()) & \
              (credit_df.YEAR == get_year())

    result_df = credit_df.join(customer_df, credit_df.CUST_SSN == customer_df.SSN, 'left') \
                         .where(filters) \
                         .orderBy(credit_df.DAY)

    result_df.show(truncate=False, n=1000)



# ---

# 2)    Used to display the number and total values of transactions for a given type.

# In[13]:


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


# In[14]:


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


# In[15]:


def transaction_total_and_no_by_type(credit_df) -> None:
    """
    Query transaction data based on the user-provided transaction type.

    Returns:
        None

    Raises:
        ValueError: If the input transaction type is not valid.

    """

    transaction_type = get_valid_input(credit_df, column_name= 'TRANSACTION_TYPE') 

    result_df = credit_df.filter(credit_df.TRANSACTION_TYPE == transaction_type) \
                             .select(count('TRANSACTION_ID').alias('Total no of transactions'), \
                                     sum('TRANSACTION_VALUE').alias('Sum of Transaction Values'))

    result_df.show(truncate=False, n=1000)



# ---

# 3)    Used to display the total number and total values of transactions for branches in a given state.

# In[16]:


def transaction_total_and_no_by_branch_on_state (branch_df, credit_df)-> None:
    """
    Process transaction data by querying based on user input of state.

    Args:
        credit_df (DataFrame): The DataFrame containing the credit data.
        customer_df (DataFrame): The DataFrame containing the customer data.

    """

    state = get_valid_input(branch_df, column_name='BRANCH_STATE')

    result_df = credit_df.join(branch_df, credit_df.BRANCH_CODE == branch_df.BRANCH_CODE, 'left') \
                         .where(branch_df.BRANCH_STATE == 'MN') \
                         .groupBy(branch_df.BRANCH_CODE) \
                         .agg(count(credit_df.TRANSACTION_ID).alias('Transaction_Count'), \
                              sum(credit_df.TRANSACTION_VALUE).alias('Transaction_Sum'))

    result_df.show(truncate=False, n=1000)



# ---

# ## 2.2 Customer Details Module

# 1) Used to check the existing account details of a customer.
# 

# In[17]:


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


# In[18]:


def show_account_details(customer_df) -> None:
    """
    Retrieves and displays account details based on user input.

    Returns:
        None
    """
    valid_column_names = customer_df.columns

    filter_column = choose_one_from(valid_column_names)
    filter_value = get_valid_input(df=customer_df, column_name=filter_column)

    result_df = customer_df.filter(customer_df[filter_column] == filter_value)
    result_df.show(truncate=False, n=1000)



# 2) Used to modify the existing account details of a customer.

# In[19]:

def create_sql_connection():
    # MySQL connection properties
    mysql_database_name = 'creditcard_capstone'
    mysql_user = 'root'
    mysql_password = 'password'
    mysql_host = 'localhost'
    mysql_port = 3306

    # Create the MySQL connection URL
    mysql_url = f'mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database_name}'

    # Create a SQLAlchemy engine
    return create_engine(mysql_url)

def update_dataframe(customer_df) -> None:
    """
    Update values in a PySpark DataFrame based on a filter condition.

    Returns:
        None.

    """
    valid_column_names = customer_df.columns

    filter_column = choose_one_from(valid_column_names, msg = 'Select Column to filter with')
    filter_value = get_valid_input(df = customer_df, column_name = filter_column)

    column_to_update = choose_one_from(valid_column_names, msg = 'Column to update')
    new_value = input("Enter new value:") # can implement appropriate input validation here
    
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
    
    


# 3) Used to generate a monthly bill for a credit card number for a given month and year.

# In[20]:


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


# In[21]:


def generate_monthly_bill(credit_df) -> DataFrame:
    """
    Generates a monthly bill based on user input.

    Returns:
        DataFrame: The resulting DataFrame containing the selected columns for the specified credit card number, month, and year.
    """
    print("Please enter the credit card number.")
    input_cc_no = get_valid_16_digit_input()
    input_year = get_year()
    input_month = get_month()

    result_df = credit_df.filter((credit_df['CREDIT_CARD_NO'] == input_cc_no) &
                                (credit_df['MONTH'] == input_month) &
                                (credit_df['YEAR'] == input_year))

    return result_df.select(['TRANSACTION_ID', 'DAY', 'MONTH', 'YEAR', 'TRANSACTION_TYPE', 'TRANSACTION_VALUE'])


# In[22]:


def print_transaction_summary(df: DataFrame) -> None:
    """
    Prints a summary of the transactions in the provided DataFrame.

    Args:
        df (DataFrame): The DataFrame containing the transactions.

    Returns:
        None
    """
    total_and_count = df.agg({'TRANSACTION_VALUE': 'SUM', 'TRANSACTION_ID': 'COUNT'}).first()
    total = total_and_count['sum(TRANSACTION_VALUE)']
    count = total_and_count['count(TRANSACTION_ID)']

    print(f'Summary: There were {count} transactions totaling $ {total}')


# In[23]:


def display_monthly_bill(credit_df) -> None:
    """
    Displays the monthly bill and transaction summary.

    Returns:
        None
    """
    result_df = generate_monthly_bill(credit_df)
    result_df.show(truncate=False, n=1000)
    print_transaction_summary(result_df)


# 4) Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.

# In[24]:
def get_date_input():
    while True:
        date_str = input("Enter a date (YYYY-MM-DD): ")
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return date_str
        except ValueError:
            print("Invalid date format. Please enter a date in the format YYYY-MM-DD.")



# In[25]:


def transaction_between_dates(credit_df) -> None:
    """
    Displays transactions between two specified dates and prints a transaction summary.

    Returns:
        None
    """
    
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



# In[26]:




# ---
