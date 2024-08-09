Clone the project

Install snowflake connector: 
	pip install snowflake-connector-python
 
Update username, password, account with your snowflakes credentials in config.json file

To create snowflake environment, execute 'init_snowflake_environment' function:
  python3 main.py init_snowflake_environment

To test aggregate table update, excute 'test_task':
  python3 main.py test_task
