import pandas as pd 
import duckdb
import os
import re 
from ollama import Client

def create_message(table_name, query):
    class message:
        def __init__(self, system, user, column_names, column_attr):
            self.system = system
            self.user = user
            self.column_names = column_names
            self.column_attr = column_attr

    system_template = """
    Given the following SQL table, your job is to write queries given a user's request.\n
    CREATE TABLE {} ({}) 
    """

    user_template = """
    Write a SQL query that returns - {}
    """

    tbl_describe = duckdb.sql("DESCRIBE SELECT * FROM " + table_name + ";")
    col_attr = tbl_describe.df()[["column_name", "column_type"]]
    col_attr["column_joint"] = col_attr["column_name"] + " " +  col_attr["column_type"]
    col_names = str(list(col_attr["column_joint"].values)).replace('[', '').replace(']', '').replace('\'', '')

    system = system_template.format(table_name, col_names)
    user = user_template.format(query)

    m = message(system=system, user=user, column_names=col_attr["column_name"], column_attr=col_attr["column_type"])
    #print(m.system)
    #print(m.user)
    return m 

def add_quotes(query, col_names):
    for i in col_names:
        if i in query:
            l = query.find(i)
            if query[l-1] != "'" and query[l-1] != '"': 
                query = str(query).replace(i, '"' + i + '"') 
    return(query)

def lang2sql(table_name, query):
    class response:
        def __init__(output, message, response, sql):
            output.message = message
            output.response = response
            output.sql = sql
    
    m = create_message(table_name = table_name, query = query)

    message = [
        {
            "role": "system",
            "content": m.system
        },
        {
            "role": "user",
            "content": m.user
        }
    ]

    ollama_response = client.chat(model='llama2:13b', messages=message, stream=False)
    sql_query = add_quotes(query = ollama_response["message"]["content"], col_names = m.column_names)
    output = response(message = m, response = ollama_response, sql = sql_query)
    return output

if __name__ == "__main__":
    client = Client(host='http://192.168.2.19:11434')

    path = "./data"
    files = [x for x in os.listdir(path = path) if ".csv" in x]
    chicago_crime = pd.concat((pd.read_csv(path +"/" + f) for f in files), ignore_index=True)
    
    query = "How many cases ended up with arrest?"
    response = lang2sql(table_name = "chicago_crime", query = query)

    sql_pattern = r"SELECT\s+.*?\s+FROM\s+.*?\s+WHERE\s+.*?;"

    # Find the SQL query in the response
    sql_query = re.search(sql_pattern, response.sql, re.DOTALL).group()

    # Print the extracted SQL query
    print("Extracted SQL query:")
    print(sql_query)

    duckdb.sql(sql_query).show()

   




