from pyspark.sql import SparkSession
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from pyspark.sql.functions import *
from datetime import datetime
import os
import sys

import __utils__

       

def data_transform(df1):
    #drop duplicates
    df1=df1.drop_duplicates()
    # Extracting max date from source data
    max_processed_date = df1.agg({"PageViewDate": "max"}).collect()[0][0]
    
    # Extracting Date and Time from PageViewDate
    df1=df1.withColumn('PageViewTime', date_format('PageViewDate', 'HH:mm:ss'))
    df1=df1.withColumn('PageViewDate', date_format('PageViewDate', 'yyyy-MM-dd'))
    
    # Inserting processed date
    df1=df1.withColumn('ProcessedDate', lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    # Extracting Device used to login to WebUi
    df1 = df1.withColumn("DeviceUsed", when(col('UserAgent').contains("Windows"),"Windows Machine")
                                     .when(col('UserAgent').contains("iPad"),"iPad")
                                     .when(col('UserAgent').contains("iPhone"),"iPhone")
                                     .when(col('UserAgent').contains("Android"),"Android Device")
                                     .when(col('UserAgent').contains("Macintosh"),"Macintosh")
                                     .when(col('UserAgent').contains("Linux"),"Linux Machine")
                                     .otherwise(None))
    
    # Extracting Booking Id
    df1=df1.withColumn('BookingId', when(col('RequestedURL').contains("complete/BND/")
                                         ,split((split(df1['RequestedURL'], 'complete/BND/').getItem(1)), '\?').getItem(0))
                                         .otherwise(None))
    
    # Extracting Trio Id
    df1=df1.withColumn('TripId', when(col('RequestedURL').contains("complete/BND/")
                                         ,split(df1['RequestedURL'], 'tripId=').getItem(1))
                                         .otherwise(None))
    # Extracting searching purpose
    df1=df1.withColumn('VisitPurpose', split(df1['RequestedURL'], 'www.abcholidays.com').getItem(1))
    df1 = df1.withColumn("VisitPurpose", when(col('VisitPurpose').contains("holiday"),"HOLIDAYS")
                                     .when(col('VisitPurpose').contains("hotel"),"HOTEL")
                                     .when(col('VisitPurpose').contains("honeymoon"),"HONEYMOON")
                                     .when(col('VisitPurpose').contains("bestfitdeal"),"BEST FIT DEAL")
                                     .otherwise(None))
    df1=df1.select(['PageViewId','CookieID','RequestedURL','RefererURL','PageViewDate','PageViewTime',
                    'UserAgent','DeviceUsed','BookingId','TripId','VisitPurpose','ProcessedDate'])
    return df1,max_processed_date

def write_let(let):
    file1 = open(<let_filename>, "w")  # write mode
    file1.write(let)
    file1.close()
    
if __name__ == "__main__":  
    try:
        # Creating log file
        dt=str(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
        logfile_name=f'LOGS/daily_batch_process_{dt}.log'
        old_stdout = sys.stdout
        log_file = open(logfile_name,"w")
        # Write Std out messages to log file
        sys.stdout = log_file
        print(f"this will be written to {logfile_name}")

        # c=Creating Spark Session
        spark=SparkSession.builder.getOrCreate()
        #Assigning table names
        source_table = <source_table>
	stage_table = <stage_table>
        target_table = <target_table>
        # Fetching last extracted time of source data
        LET=spark.read.csv(<let_filename>,header=False,inferSchema=True).collect()[0]['_c0']
        # Creating extraction query
        extraction_query = f"select * from {source_table} where PageViewDate > '{LET}' "
        #Extracting data from source sql server
        #df = __utils__.read_table(query=extraction_query, spark)  
        df=spark.read.csv('WebTrackingData.csv',header=True,inferSchema=True)
        print('Data extracted from source')
        if df :
	    # Cleanse and load data to staging table
	    del_stage_sql= f"truncate table if exists {stage_table} "
            __utils__.run_qry(del_stage_sql)
	    # Loading data to snowflake stage table
            __utils__.write_data_snowflake(df,stage_table)
            # Delete the data from target table for those dates which is about to process to avoid duplicates
            del_sql= f"DELETE FROM {target_table} where PageViewDate > '{LET}' "
            __utils__.run_qry(del_sql)
            # Performing basing transformations and extrating new LET
            df,new_let = data_transform(df)
            print('Extracted data has been transformed')
            # Loading data to snowflake
            __utils__.write_data_snowflake(df,target_table)
            print('Transformed data loaded to destination')
            # Saving the new LET
            write_let(new_let)
            print('Wrote new LET to let file')
        else:
            print('No New data found')
    except Exception as e: 
        print(f'Process failed with error: {e}')
    finally:
        sys.stdout = old_stdout
        log_file.close()
