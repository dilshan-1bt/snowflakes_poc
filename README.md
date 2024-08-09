
**Prerequisite**

	Snowflake account

**Clone the project**

	git clone  https://github.com/dilshan-1bt/snowflakes_poc.git

**Install snowflake connector:**

	pip install snowflake-connector-python
 
**Update username, password, account with your snowflakes credentials in config.json file**

**To create snowflake environment, execute 'init_snowflake_environment' function:** It will create Wharehouse, Database, Schema, Tables, Aggrigate Tables, Stream, View and Task
	
	python3 main.py init_snowflake_environment

**To test aggregate table update, excute 'test_task':** I will insert another 5000 records to Customer_lookup table

	python3 main.py test_task


https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-install

https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example

https://docs.snowflake.com/en/sql-reference/commands-table
