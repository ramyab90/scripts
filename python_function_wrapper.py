from postgres_db import *

# Execution Example
file_path = "C:\\Users\\Hari\\ramya\\ContainerSolutions\\Scripts\\titanic.csv"
table_name = 'titanic_user_details'
dbname = 'titanic'
#host = 'http://127.0.0.1:51926/?key=40b2789d-fd69-4c61-b6f8-890dfddc4b5c'
host = 'localhost'
port = '5432'
user = 'postgres'
pwd = 'Admin@123'

#pg_create_table(file_path, table_name, dbname, host, port, user, pwd)

#pg_load_table(file_path, table_name, dbname, host, port, user, pwd)