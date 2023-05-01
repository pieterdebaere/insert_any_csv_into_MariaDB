import mysql.connector
import csv
import os
import re
from dotenv import load_dotenv

load_dotenv()

def connect_to_mariadb():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        
        print("Successfully connected to MariaDB.")
        return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        return None

def infer_data_type(value):
    if re.match(r'^-?\d+$', value):
        return 'INT'
    elif re.match(r'^-?\d+\.\d+$', value):
        return 'FLOAT'
    else:
        return 'TEXT'
    
def is_integer(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

def create_table(connection, table_name, columns):
    cursor = connection.cursor()
    
    column_defs = ', '.join([f'`{column_name}` {data_type}' for column_name, data_type in columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({column_defs});'
    
    try:
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table '{table_name}' created successfully.")
    except mysql.connector.Error as e:
        print(f"Error creating table '{table_name}': {e}")
        connection.rollback()

def insert_data(connection, table_name, headers, inferred_data_types, rows):
    cursor = connection.cursor()
    
    for row in rows:
        escaped_values = [value.replace("'", "''") for value in row]
        formatted_values = []
        for header, value, data_type in zip(headers, escaped_values, inferred_data_types):
            if data_type == 'INT' and not is_integer(value):
                formatted_values.append('NULL')
            else:
                formatted_values.append(f"'{value}'")
        values = ', '.join(formatted_values)
        insert_query = f'INSERT INTO `{table_name}` ({", ".join([f"`{header}`" for header in headers])}) VALUES ({values});'
        
        try:
            cursor.execute(insert_query)
        except mysql.connector.Error as e:
            print(f"Error inserting data into '{table_name}': {e}")
            connection.rollback()
            return
    
    connection.commit()
    print(f"Data inserted into '{table_name}' successfully.")

def main():
    connection = connect_to_mariadb()
    if connection:
        csv_file = 'vgsales.csv'
        documentname = os.path.splitext(csv_file)[0]

        with open(csv_file, 'r', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            headers = next(csv_reader)
            first_row = next(csv_reader)
            rows = [first_row] + list(csv_reader)

        columns = [(header, infer_data_type(value)) for header, value in zip(headers, first_row)]
        inferred_data_types = [data_type for _, data_type in columns]
        create_table(connection, documentname, columns)
        insert_data(connection, documentname, headers, inferred_data_types, rows)

        connection.close()

if __name__ == "__main__":
    main()