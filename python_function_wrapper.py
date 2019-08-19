from postgres_db import *

# Script input
file_path = "C:\\Users\\Hari\\ramya\\ContainerSolutions\\Scripts\\titanic.csv"
table_name = 'titanic_user_details'
dbname = 'titanic'
#host = 'http://127.0.0.1:51926/?key=40b2789d-fd69-4c61-b6f8-890dfddc4b5c'
host = 'localhost'
port = '5432'
user = 'postgres'
pwd = 'Admin@123'
Name = 'Miss. Laina Heikkinen'
Age = '10'

user_query = "deleteRecord"

if user_query == 'createTable':
  print("Script will create the postgres table...")
  pg_create_table(dbname, host, port, user, pwd)

elif user_query == 'loadTable':
  print("Script will load data to the postgres table...")
  pg_load_table(file_path, table_name, dbname, host, port, user, pwd)

elif user_query == 'readTable':
  print("Script will read data from the postgres table...")
  pg_read_table(dbname, host, port, user, pwd)

elif user_query == 'updateTable':
  print("Script will update data to the postgres table...")
  pg_update_table(dbname, host, port, user, pwd, Name, Age)

elif user_query == 'deleteRecord':
  print("Script will delete data from the postgres table...")
  pg_delete_table(dbname, host, port, user, pwd, Name)

else:
  print("Error: User provided an undefined input!")