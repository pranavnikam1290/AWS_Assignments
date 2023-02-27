import boto3
import pandas as pd
#import xlrd
from botocore.exceptions import ClientError
import os
import sys

import pyarrow as pa
from s3fs import S3FileSystem
import pyarrow.parquet as pq

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from datetime import date
from botocore.exceptions import ClientError
from pyspark.sql.functions import count, col, weekofyear, year, lit, concat, sum, min, max, avg, to_date, coalesce, first, last,date_format,to_timestamp, dayofmonth, month, quarter
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

from pyspark.sql import functions as f
from pyspark.sql.functions import col,concat_ws,row_number
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sc.setLogLevel("ERROR")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)




#Funcion to get boto3 client
def get_client():
    print("inside boto3")
    return boto3.client('s3')
    
def get_bucket(bucket):
    print("inside my bucket")
    s3 = boto3.resource('s3')
    return s3.Bucket(bucket)
    
#Funcion to get s3 resource
def get_resource():
    return boto3.resource('s3')

def move_file(bucketname,source,target):
    s3 = get_resource()
    my_bucket = get_bucket(bucketname)
    print("inside move file")
    for obj in my_bucket.objects.filter(Prefix = source):
        source_filename = (obj.key).split('/')[-1]
        copy_source = {
            'Bucket': bucketname,
            'Key': obj.key
        }
        target_filename = "{}/{}/{}".format(target,source_filename.split(".")[0],source_filename)
        s3.meta.client.copy(copy_source, bucketname, target_filename)
        
        #s3.Object(bucketname, obj.key).delete()
    target_file = "s3://"+bucketname+"/"+target_filename
    print("Target file: ", target_file)
    print("target_filename: ", target_filename)
    print("move file completed")
    return target_file
    
def read_csv_to_dataframe(bucketname, target, target_file):
    print("read_csv_to_dataframe started")
    if target_file.endswith('.csv'):
        df_csv = pd.DataFrame(pd.read_csv("s3://"+bucketname+"/"+target+"/"+target_file.split('/')[-1].split('.')[0]+"/"+target_file.split('/')[-1].split('.')[0]+".csv"))
    else:
        print("Target file is not csv")
    print("convert_xls_to_csv ended")
    df_csv = df_csv.dropna()
    read_file_spark=spark.createDataFrame(df_csv)#Remove empty rows
    read_file_spark = read_file_spark.withColumnRenamed("Adj Close", "Adj_Close") \
            .withColumnRenamed("Script name", "Script_name")
    read_file_spark = read_file_spark.withColumn("Open",col("Open").cast((DoubleType()))).withColumn("High",col("High").cast((DoubleType()))).withColumn("Low",col("Low").cast((DoubleType()))).withColumn("Close",col("Close").cast((DoubleType())))
    
    print(read_file_spark)
    #df_csv[col] = df_csv[col].astype(float)
    return df_csv

#Create and Run crawler function        
def create_and_run_crawler(regionName,CrawlerName,UserRole,S3DatabaseName, path):
    print("inside crawler table")
    glue_client = boto3.client(
        'glue', 
        region_name = regionName
    )
    
    
    try:
        glue_client.create_crawler(
            Name = CrawlerName+"-"+path.split('/')[-1].split('.')[0] ,
            Role = UserRole,
            DatabaseName = S3DatabaseName,
            Targets = 
            {
                'S3Targets': 
                [
                    {   
                        'Path':path,
                        'Exclusions': [
                        ]
                    },
                ]
            }
        )
        glue_client.start_crawler(
            Name = CrawlerName
        )
    except:
        glue_client.update_crawler(
            Name = CrawlerName+"-"+path.split('/')[-1].split('.')[0],
            Role = UserRole,
            DatabaseName = S3DatabaseName,
            Targets = 
            {
                'S3Targets': 
                [
                    {
                        'Path':path,
                        'Exclusions': [
                        ]
                    }
                ],
            }
        )
        glue_client.start_crawler(
            Name = CrawlerName+"-"+path.split('/')[-1].split('.')[0]
        )

def check_header_columns(bucketname,header_path, df, target_file):
    print("check_header_columns started")
    s3 = get_resource()
    my_bucket = get_bucket(bucketname)
    header_path = header_path +"/"+target_file.split('/')[-1].split('.')[0]+"/Header_File.csv"
    obj = my_bucket.Object(key= header_path) 
    df_header = pd.read_csv(obj.get()['Body'])
    
    print(set(df.columns))
    print(set(df_header.columns))
    
    if (set(df.columns.array[0].split('|'))) == (set(df_header.columns)):
        print("Column Headers are equal")
    else:
        print("Column Headers are not equal")
    print("check_header_columns ended")

def check_counter_file(bucketname,counter_filepath,df,target_file):
    print("check_counter_file started")
    s3 = get_resource()
    my_bucket = get_bucket(bucketname)
    counter_filepath = counter_filepath+"/"+target_file.split('/')[-1].split('.')[0]+"/Counter_File.txt"
    obj_counter = my_bucket.Object(key=counter_filepath) 
    df_Counter = pd.read_csv(obj_counter.get()['Body'], header = None)
    print(df_Counter)
    if (int(str(df_Counter[0][0]).split(":")[1])) == (len(df)):
        print("Column Headers are equal")
    else:
        print("Column Headers are not equal")
    print(len(df))
    print(df_Counter)
    print("check_counter_file ENDED")


def transform_dataframe(pandasDF,src_path,sep):
    #sparkDF=spark.createDataFrame(pandasDF)
    

        
    print("inside transfomr_dataframe")
    
    sparkDF = spark.read.csv(src_path, sep=sep , header=True ,inferSchema='True')
    print(sparkDF.show(5))
    
    sparkDF=sparkDF.withColumn('Date',to_date(col('Date'),'M/d/yyyy'))
    print("date transfomr_dataframe")
    sparkDF = sparkDF.withColumnRenamed("Adj Close", "Adj_Close") \
        .withColumnRenamed("Scrip name", "Script_name")
    df_output = sparkDF.withColumn('Day',dayofmonth(sparkDF.Date))\
        .withColumn('Year',year(sparkDF.Date))\
        .withColumn('Month',month(sparkDF.Date))\
        .withColumn('Quarter',quarter(sparkDF.Date))
    print("outside transfomr_dataframe")
    return df_output

def create_parquet_file(bucketname, src_path, df_spark, target_file_landing):
    print("create_parquet_file started")
  
    df_spark.write.mode("Overwrite").parquet("s3://"+bucketname+"/"+src_path+"/"+target_file_landing.split("/")[-1].split(".")[0])
    
    print(df_spark.show(5))
    print("create_parquet_file ended")
    #return(df_spark)

def write_spark_dataframe(bucketname,df_input, delim, file_path,target_file_landing):
    file_path = "s3://"+bucketname+"/"+file_path + "/" + target_file_landing.split('/')[-1].split('.')[0] + "/" + target_file_landing.split('/')[-1].split('.')[0] +"-updated.csv"
    print(file_path)
    df_input.write.option("header",True)\
        .option("delimiter",delim)\
        .csv(file_path)
    return file_path
        
if __name__ == "__main__":
    bucketname = "saama-gene-training-data-bucket"
    Input = "pranav_nikam/Use_Case_3/Input"
    preprocess = "pranav_nikam/Use_Case_3/Preprocess"
    landing = "pranav_nikam/Use_Case_3/Landing"
    standardised = "pranav_nikam/Use_Case_3/Standardized"
    outbound = "pranav_nikam/Use_Case_3/Outbound"
    
    CrawlerName = 'saama-gene-training-pranav-crawler'
    UserRole = 'saama-gene-training-glue-service-role'
    S3DatabaseName = 'saama-gene-training-data'
    regionName = 'ap-south-1'
    CrawlerName_updated = 'saama-gene-training-pranav-crawler-updated'
    
    sep =','
    
    
    target_file = move_file(bucketname,Input,preprocess)
    print(target_file)
    df_csv = read_csv_to_dataframe(bucketname, preprocess, target_file)
    print(df_csv)
    check_header_columns(bucketname,preprocess, df_csv,target_file)
    check_counter_file(bucketname,preprocess,df_csv,target_file)
    target_file_landing = move_file(bucketname,preprocess,landing)
    print(f"target file landing:{target_file_landing}",target_file_landing)
    crawler_path = "s3://"+bucketname+"/"+landing+"/"+target_file_landing.split('/')[-1].split('.')[0]
    create_and_run_crawler(regionName,CrawlerName,UserRole,S3DatabaseName,crawler_path)
    
    output_df = transform_dataframe(df_csv,target_file_landing,sep)
    print(output_df)
    create_parquet_file(bucketname, standardised, output_df, target_file_landing)
    crawler_path_new = "s3://"+bucketname+"/"+outbound+"/"+target_file_landing.split('/')[-1].split('.')[0]
    
    write_spark_dataframe(bucketname,output_df,sep,outbound,target_file_landing)
    
    create_and_run_crawler(regionName,CrawlerName_updated,UserRole,S3DatabaseName,crawler_path_new)
   
