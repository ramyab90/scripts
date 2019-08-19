import psycopg2
import pandas as pd
import sys

def pg_create_table(dbname, host, port, user, pwd):
    '''
    This function will create the target table in postgres!
    '''
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port,\
         user=user, password=pwd)
        print("Connecting to Database to create table...")
        cur = conn.cursor()

        #create table
        create_table_query = '''CREATE TABLE titanic_user_details
          (Survived INTEGER NOT NULL,
          Pclass INTEGER NOT NULL,
          Name TEXT NOT NULL,
          Sex TEXT NOT NULL,
          Age NUMERIC NOT NULL,
          Siblings_Spouses_Aboard INTEGER NOT NULL,
          Parents_Children_Aboard INTEGER NOT NULL,
          Fare NUMERIC NOT NULL
          ); '''
    
        cur.execute(create_table_query)
        cur.execute("commit;")
        print("Table created successfully in PostgreSQL")
        conn.close()
        print("DB connection closed")

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)

def pg_load_table(file_path, table_name, dbname, host, port, user, pwd):
    '''
    This function will load the data to postgres table!
    '''
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port,\
         user=user, password=pwd)
        print("Connecting to Database to load data...")
        cur = conn.cursor()
        f = open(file_path, "r")
        # Truncate the table first
        cur.execute("Truncate {} Cascade;".format(table_name))
        print("Truncated {}".format(table_name))
        # Load table from the file with header
        cur.copy_expert("copy {} from STDIN CSV HEADER QUOTE '\"'".format(table_name), f)
        cur.execute("commit;")
        print("Loaded data into {}".format(table_name))
        conn.close()
        print("DB connection closed.")

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)

def pg_read_table(dbname, host, port, user, pwd):
    '''
    This function will read data from postgres table!
    '''
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port,\
         user=user, password=pwd)
        print("Connecting to Database to read data...")
        cur = conn.cursor()

        sql_read_query = "SELECT * FROM titanic_user_details LIMIT 1;"
        cur.execute(sql_read_query)

        print("List the top 5 rows")
        records = cur.fetchall() 
        
        print("Print each row and it's columns values")
        for row in records:
            print("Survived = ", row[0], )
            print("Pclass = ", row[1])
            print("Name = ", row[2])
            print("Sex = ", row[3])
            print("Age = ", row[4])
            print("Siblings_Spouses_Aboard = ", row[5])
            print("Parents_Children_Aboard = ", row[6])
            print("Fare  = ", row[7 ], "\n")

        conn.close()
        print("DB connection closed.")

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)

def pg_update_table(dbname, host, port, user, pwd, Name, Age):
    '''
    This function will update the data from postgres table!
    '''
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port,\
         user=user, password=pwd)
        print("Connecting to Database to update data...")
        cur = conn.cursor()

        sql_update_query = """UPDATE titanic_user_details SET Age = %s WHERE Name = %s"""
        cur.execute(sql_update_query, (Age, Name))
        conn.commit()
        #count = cursor.rowcount
        print("Record updated successfully")        

        conn.close()
        print("DB connection closed.")

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)

def pg_delete_table(dbname, host, port, user, pwd, Name):
    '''
    This function will delete the data from postgres table!
    '''
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port,\
         user=user, password=pwd)
        print("Connecting to Database to delete data...")
        cur = conn.cursor()

        sql_delete_query = """DELETE FROM titanic_user_details WHERE Name = %s"""
        cur.execute(sql_delete_query, (Name,))
        conn.commit()

        print("Record deleted successfully")        

        conn.close()
        print("DB connection closed.")

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)

# # Execution Example
# file_path = "C:\\Users\\Hari\\ramya\\ContainerSolutions\\Scripts\\titanic.csv"
# table_name = 'titanic_user_details'
# dbname = 'titanic'
# #host = 'http://127.0.0.1:51926/?key=40b2789d-fd69-4c61-b6f8-890dfddc4b5c'
# host = 'localhost'
# port = '5432'
# user = 'postgres'
# pwd = 'Admin@123'
# Name = 'Miss. Laina Heikkinen'
# Age = '22'