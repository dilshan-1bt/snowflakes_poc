import snowflake.connector
import csv
import json

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

warehouse_name = config['snowflake']['configs']['warehouse_name']
database_name = config['snowflake']['configs']['database_name']
schema_name = config['snowflake']['configs']['schema_name']

customer_lookup_table = config['snowflake']['tables']['customer_lookup_table']
product_lookup_table = config['snowflake']['tables']['product_lookup_table']
sales_data_table = config['snowflake']['tables']['sales_data_table']
agg_customer_gender_count = config['snowflake']['tables']['agg_customer_gender_count']

stream_customerlookup_insert = config['snowflake']['configs']['stream_customerlookup_insert']
view_customerlookup_changes = config['snowflake']['configs']['view_customerlookup_changes']
task_update_gender_count = config['snowflake']['configs']['task_update_gender_count']



# Create customer_lookup table
def create_customer_lookup():

    connection = snowflake.connector.connect(
        user=config['snowflake']['auth']['username'],
        password=config['snowflake']['auth']['password'],
        account=config['snowflake']['auth']['account']
    )

    cursor = connection.cursor()

    cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")

    cursor.execute(f"""CREATE TABLE IF NOT EXISTS  {customer_lookup_table}(
    CustomerKey INT PRIMARY KEY,
    Prefix STRING,
    FirstName STRING,
    LastName STRING,
    BirthDate STRING,
    MaritalStatus STRING,
    Gender STRING,
    EmailAddress STRING,
    AnnualIncome INT,
    TotalChildren INT,
    EducationLevel STRING,
    Occupation STRING,
    HomeOwner STRING
    )
    """)

    cursor.close()
    connection.close()

# Create product_lookup table
def create_produc_lookup():

    connection = snowflake.connector.connect(
        user=config['snowflake']['auth']['username'],
        password=config['snowflake']['auth']['password'],
        account=config['snowflake']['auth']['account']
    )
    cursor = connection.cursor()

    cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")

    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {product_lookup_table} (
    ProductKey INT PRIMARY KEY,
    ProductSubcategoryKey INT,
    ProductSKU STRING,
    ProductName STRING,
    ModelName STRING,
    ProductDescription STRING,
    ProductColor STRING,
    ProductSize STRING,
    ProductStyle STRING,
    ProductCost DOUBLE,
    ProductPrice DOUBLE
    )
    """)

    cursor.close()
    connection.close()


# Create sales_data table
def create_sales_data():

    connection = snowflake.connector.connect(
        user=config['snowflake']['auth']['username'],
        password=config['snowflake']['auth']['password'],
        account=config['snowflake']['auth']['account']
    )

    cursor = connection.cursor()

    cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")

    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {sales_data_table} (
    OrderDate DATE,
    StockDate DATE,
    OrderNumber STRING,
    ProductKey INT,
    CustomerKey INT,
    TerritoryKey INT,
    OrderLineItem INT,
    OrderQuantity INT
    )
    """)

    cursor.close()
    connection.close()


# Create aggregate table to keep gender counts
def create_agg_table_customer_lookup():
    connection = snowflake.connector.connect(
        user=config['snowflake']['auth']['username'],
        password=config['snowflake']['auth']['password'],
        account=config['snowflake']['auth']['account']
    )

    cursor = connection.cursor()

    cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")

    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {agg_customer_gender_count} (
    GENDER STRING,
    G_COUNT INT
    )
    """)
    
    cursor.close()
    connection.close()


# Create stream for capture customer_lookup table changes
def create_stream_customerlookup_change():
    connection = snowflake.connector.connect(
        user=config['snowflake']['auth']['username'],
        password=config['snowflake']['auth']['password'],
        account=config['snowflake']['auth']['account']
    )

    cursor =  connection.cursor()
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")
    cursor.execute(f"create or replace stream {stream_customerlookup_insert} on table {customer_lookup_table} ;")
    cursor.close()
    connection.close()


# Create view
def create_view_customerlookup_change():
    connection = snowflake.connector.connect(
        user=config['snowflake']['auth']['username'],
        password=config['snowflake']['auth']['password'],
        account=config['snowflake']['auth']['account']
    )

    cursor =  connection.cursor()
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"USE SCHEMA  {schema_name}")
    cursor.execute(f"""
    create or replace view {view_customerlookup_changes}(
	    GENDER,
	    G_COUNT
    ) as 
    select gender, count(gender) as g_count from (select gender 
    from {stream_customerlookup_insert} 
    where metadata$action = 'INSERT' and metadata$isupdate = 'FALSE') group by gender;""")
    cursor.close()
    connection.close()


# Create task to update aggregate table
# Task is schedule to execute within every 1 minute
def create_task_update_agg_gender_table():
    connection = snowflake.connector.connect(
        user=config['snowflake']['auth']['username'],
        password=config['snowflake']['auth']['password'],
        account=config['snowflake']['auth']['account']
    )

    cursor =  connection.cursor()
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")
    cursor.execute(f"""
    create or replace task {task_update_gender_count}
	warehouse={warehouse_name}
	schedule='1 minute'
	when SYSTEM$STREAM_HAS_DATA('stream_customerlookup_insert')
	as merge into {agg_customer_gender_count} acgc
    using {view_customerlookup_changes} clcv
    on acgc.gender = clcv.gender
    when matched then
        update set acgc.g_count = acgc.g_count + clcv.g_count
    when not matched then
        insert (gender, g_count) values(clcv.gender, clcv.g_count);
    """)

    # Initially the task is in suspended mode and, need to resume task
    cursor.execute(f"alter task {task_update_gender_count} resume;")
    
    cursor.close()
    connection.close()


# Read CSV and insert data to customer_lookup
def insert_customer_lookup_data(file_name):
    connection = snowflake.connector.connect(
        user=config['snowflake']['auth']['username'],
        password=config['snowflake']['auth']['password'],
        account=config['snowflake']['auth']['account']
    )

    cursor =  connection.cursor()
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")

    batch_size = 10000
    batch = []

    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        next(reader) 
        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                cursor.executemany(f"""
            INSERT INTO {customer_lookup_table} (CustomerKey, Prefix, FirstName, LastName, BirthDate, MaritalStatus, Gender, EmailAddress, AnnualIncome, 
            TotalChildren, EducationLevel, Occupation, HomeOwner) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
                connection.commit()
                batch = []

        if batch:
            cursor.executemany(f"""
            INSERT INTO {customer_lookup_table} (CustomerKey, Prefix, FirstName, LastName, BirthDate, MaritalStatus, Gender, EmailAddress, AnnualIncome, 
            TotalChildren, EducationLevel, Occupation, HomeOwner) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            connection.commit()

    cursor.close()
    connection.close()


# Read CSV and insert data to product_lookup
def insert_product_lookup_data(file_name):
    connection = snowflake.connector.connect(
        user=config['snowflake']['auth']['username'],
        password=config['snowflake']['auth']['password'],
        account=config['snowflake']['auth']['account']
    )

    cursor =  connection.cursor()
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")

    batch_size = 1000
    batch = []

    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        next(reader) 
        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                cursor.executemany(f"""INSERT INTO {product_lookup_table} (ProductKey, ProductSubcategoryKey, ProductSKU, ProductName, ModelName, ProductDescription, ProductColor, ProductSize, ProductStyle, ProductCost, ProductPrice) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", batch)
                connection.commit()
                batch = []

        if batch:
            cursor.executemany(f"""INSERT INTO {product_lookup_table} (ProductKey, ProductSubcategoryKey, ProductSKU, ProductName, ModelName, ProductDescription, ProductColor, ProductSize, ProductStyle, ProductCost, ProductPrice) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", batch)
            connection.commit()

    cursor.close()
    connection.close()


# Read CSV and insert data to sales_data
def insert_sales_data_data(file_name):
    connection = snowflake.connector.connect(
        user=config['snowflake']['auth']['username'],
        password=config['snowflake']['auth']['password'],
        account=config['snowflake']['auth']['account']
    )

    cursor =  connection.cursor()
    cursor.execute(f"USE DATABASE {database_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")

    batch_size = 1000
    batch = []

    with open(file_name, 'r') as file:
        reader = csv.reader(file)
        next(reader) 
        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                cursor.executemany(f"""INSERT INTO {sales_data_table} (OrderDate, StockDate, OrderNumber, ProductKey, CustomerKey, TerritoryKey, OrderLineItem, OrderQuantity) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", batch)
                connection.commit()
                batch = []

        if batch:
            cursor.executemany(f"""INSERT INTO {sales_data_table} (OrderDate, StockDate, OrderNumber, ProductKey, CustomerKey, TerritoryKey, OrderLineItem, OrderQuantity) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", batch)
            connection.commit()



