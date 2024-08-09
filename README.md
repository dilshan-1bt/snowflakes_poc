Clone the project

Install snowflake connector: 
	**pip install snowflake-connector-python**
 
Update username, password, account with your snowflakes credentials in config.json file

To create snowflake environment, execute 'init_snowflake_environment' function: _It will create Wharehouse, Database, Schema, Tables, Aggrigate Tables, Stream, View and Task_
  **python3 main.py init_snowflake_environment**

To test aggregate table update, excute 'test_task': I will insert another 5000 records to Customer_lookup table.
  **python3 main.py test_task**
