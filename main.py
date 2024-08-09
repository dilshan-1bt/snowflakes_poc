import snowflakeConnector as snowflakeConnector
import sys

def init_snowflake_environment():
    # Creat tables
    snowflakeConnector.create_customer_lookup()
    snowflakeConnector.create_produc_lookup()
    snowflakeConnector.create_sales_data()
    snowflakeConnector.create_agg_table_customer_lookup()

    # Create stream, view, task
    snowflakeConnector.create_stream_customerlookup_change()
    snowflakeConnector.create_view_customerlookup_change()
    snowflakeConnector.create_task_update_agg_gender_table()

    # Insert data into tables
    snowflakeConnector.insert_customer_lookup_data('./csv/CustomerLookup.csv')
    snowflakeConnector.insert_product_lookup_data('./csv/ProducLookup.csv')
    snowflakeConnector.insert_sales_data_data('./csv/SalesData.csv')

def test_task():
    # Insert another 5000 records and check the aggregate table is updated after execute the task
    snowflakeConnector.insert_customer_lookup_data('./csv/CustomerLookup_test.csv')


if __name__ == '__main__':
    globals()[sys.argv[1]]()