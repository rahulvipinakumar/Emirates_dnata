import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import time

#read data from sql server
def read_table(query, spark):
    return spark.read.format("jdbc") \
        .option("url",
                'jdbc:sqlserver://<server ip>:<port>;databaseName=<db_name>') \
        .option("user", <userid>) \
        .option("password", <password>) \
        .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver') \
        .option("query", query) \
        .load()

#write data into Snowflake table       
def write_data_snowflake(df,table):     
    SNOWFLAKE_SOURCE_NAME="net.snowflake.spark.snowflake"
    
    sfOptions =  {
                'sfURL': <snowflake url>,
                'sfUser': <userid>,
                'sfPassword': <password>,
                "sfDatabase" : <db_name>,
                "sfSchema":<schema>
                
            }
    table=table.replace('-','_')
    df.write.format(SNOWFLAKE_SOURCE_NAME) \
    .mode("append")\
    .options(**sfOptions)\
    .option("dbtable", table)\
    .save()
    
    print(f'Populated the table:{table}')

#Connection string for Snowflake  
def get_snowflake_connection(user=<userid>, password=<password>, account=<account>,
                     warehouse='COMPUTE_WH', database=<db_name>, schema=<schema>, role=<role>):
    snowflake.connector.paramstyle='qmark'
    c =snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role
    )
    return c
 
# Executing query in snowflake 
def run_qry(q, connection=None, print_q=True, data=None):
    c = connection if connection else get_snowflake_connection()
    print("Qry is", q, end=' ... ') if print_q  else None
    try:
        t1 = time.time()
        data = c.cursor().execute(q).fetch_pandas_all()
        data.columns = data.columns.str.lower()
        print(f'done in {time.time()-t1:.2f} seconds.') if print_q  else None
    except snowflake.connector.errors.NotSupportedError:
        # fetch_pandas_all is not supported for queries that dont have any data
        # such as Insert, Update, Merge, SPs etc.
        print(f'done in {time.time()-t1:.2f} seconds.') if print_q  else None
    except snowflake.connector.errors.ProgrammingError as e:
        print('timeout')
        c.rollback()
        c.commit()
        raise(e)
    c.commit()
    c.close() if connection is None else None 
    return data 
    
    
