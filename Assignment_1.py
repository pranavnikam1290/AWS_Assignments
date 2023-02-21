import boto3
import pandas as pd
import xlrd
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
from pyspark.sql.functions import count, col, weekofyear, year, lit, concat, sum, min, max, avg, to_date, coalesce, first, last,date_format,to_timestamp
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
    
def my_bucket(bucket):
    print("inside my bucket")
    s3 = boto3.resource('s3')
    return s3.Bucket(bucket)
    
#Funcion to get s3 resource
def get_resource():
    return boto3.resource('s3')

def move_file(bucketname,source,target,my_bucket):
    s3 = get_resource()
    print("inside move file")
    for obj in my_bucket.objects.filter(Prefix = source):
        source_filename = (obj.key).split('/')[-1]
        copy_source = {
            'Bucket': bucketname,
            'Key': obj.key
        }
        target_filename = "{}/{}".format(target,source_filename)
        s3.meta.client.copy(copy_source, bucketname, target_filename)
        
        #s3.Object(bucketname, obj.key).delete()
    target_file = "s3://"+bucketname+"/"+target_filename
    print("Target file: ", target_file)
    print("target_filename: ", target_filename)
    print("move file completed")
    return target_file

#print("s3://"+bucketname+"/"+target+"/saama-gene-training-pranav"+target_filename.split('/')[-1].split('.')[0]+".csv")
def convert_xls_to_csv(bucketname, target, target_file):
    print("convert_xls_to_csv started")
    if target_file.endswith('.xls'):
        print("True")
        read_file = pd.read_excel (target_file)
            
        read_file.to_csv("s3://"+bucketname+"/"+target+"/saama-gene-training-pranav"+target_file.split('/')[-1].split('.')[0]+".csv", index = None,header=True,sep="|")
                      
        df1 = pd.DataFrame(pd.read_csv("s3://"+bucketname+"/"+target+"/saama-gene-training-pranav"+target_file.split('/')[-1].split('.')[0]+".csv"))
    print("convert_xls_to_csv ended")
    return df1

def check_header_columns(my_bucket,header_path, df):
    print("check_header_columns started")
    obj = my_bucket.Object(key= header_path) 
    df_header = pd.read_csv(obj.get()['Body'])
    
    print(set(df.columns))
    print(set(df_header.columns))
    
    if (set(df.columns.array[0].split('|'))) == (set(df_header.columns)):
        print("Column Headers are equal")
    else:
        print("Column Headers are not equal")
    print("check_header_columns ended")

def check_counter_file(my_bucket,counter_filepath,df):
    print("check_counter_file started")
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

#for obj in my_bucket.objects.filter(Prefix = source_preprocess):
    
#    source_filename = (obj.key).split('/')[-1]
#    print(source_filename)
#    copy_source = {
#        'Bucket': bucketname,
#        'Key': obj.key
#    }
#    target_filename_landing = "{}/{}".format(target_landing,source_filename)
#    s3.meta.client.copy(copy_source, bucketname, target_filename_landing)
    
      
    #s3.Object(bucketname, obj.key).delete()
#target_file_landing = "s3://"+bucketname+"/"+target_filename_landing



#print(target_file_landing)


def create_and_run_crawler(regionName,CrawlerName,UserRole,S3DatabaseName, Path):
    print("create_and_run_crawler started")
    glue_client = boto3.client(
        'glue', 
        region_name = regionName
    )
    
    
    try:
        glue_client.create_crawler(
            Name = CrawlerName ,
            Role = UserRole,
            DatabaseName = S3DatabaseName,
            Targets = 
            {
                'S3Targets': 
                [
                    {
                        'Path':Path
                    }
                ]
            }
        )
        glue_client.start_crawler(
            Name = CrawlerName
        )
    except:
        glue_client.update_crawler(
            Name = CrawlerName,
            Role = UserRole,
            DatabaseName = S3DatabaseName,
            Targets = 
            {
                'S3Targets': 
                [
                    {
                        'Path':Path
                    }
                ]
            }
        )
        glue_client.start_crawler(
            Name = CrawlerName
        )
    print("create_and_run_crawler ended")
"""
table = pa.Table.from_pandas(df1)
output_file = "s3://"+bucketname+"pranav_nikam/Standardized/INFY/output.parquet"  # S3 Path need to mention
s3 = S3FileSystem()

pq.write_to_dataset(table=table,
                    root_path=output_file,partition_cols=['Date'],
                    filesystem=s3)

print("File converted from CSV to parquet completed")
"""
def create_parquet_file(bucketname, src_path):
    print("create_parquet_file started")
    Schema = StructType([
        StructField('Script name', StringType()),
        StructField('Date', StringType()),
        StructField('Open', DoubleType()),
        StructField('High', DoubleType()),
        StructField('Low', DoubleType()),
        StructField('Close', DoubleType()),
        StructField('Adj Close', DoubleType()),
        StructField('Volume', DoubleType()),
        ])
        
    #src_path = target_file_landing
    
    df_spark = spark.read.csv(src_path, sep='|', header=True, schema=Schema)
    df_spark = df_spark.withColumnRenamed("Adj Close", "Adj_Close") \
            .withColumnRenamed("Script name", "Script_name")
            
    df_spark.write.partitionBy("Script_name").mode("Overwrite").parquet("s3://"+bucketname+"/"+"pranav_nikam/Standardized/"+src_path.split("/")[-1].split(".")[0])
    
    print(df_spark.show(5))
    print("create_parquet_file ended")
    return(df_spark)
#df_spark=df_spark.withColumn('Date',to_date(col('Date'),'yyyy-MM-dd')).withColumn("Volume",col("Volume").cast((IntegerType())))

#print(df_spark.show(5))
def create_summary_file(df_spark, target_file_landing):
    print("create_summary_file started")
    src_path = target_file_landing
    
    #df_spark = spark.read.csv(src_path, sep='|', header=True)

    df_output = df_spark.withColumn('week_of_year',weekofyear(df_spark.Date)).alias("Week_No")
    
    df_output = df_output.withColumn("Date",to_timestamp(col("Date"))).withColumn("year", date_format(col("Date"), "yyyy"))
    
    df_output = df_output.withColumn("Week_No", concat(lit("Week-"), col("week_of_year")))
    
    df_output=df_output.select(concat_ws('_',df_output.Week_No,df_output.year).alias("Week_No"),"Script_name","Date","Open","High","Low","Close","Adj_Close","Volume")
    
    print(df_output.show(5))
    
    window  = Window.partitionBy("Week_No")
    windowSpec  = Window.partitionBy("Week_No").orderBy("Date")
    
    df_summary = df_output.withColumn("row",row_number().over(windowSpec)) \
      .withColumn("St_Date", min(col("Date")).over(window)) \
      .withColumn("Open", first(col("Open")).over(window)) \
      .withColumn("End_Date", max(col("Date")).over(window)) \
      .withColumn("Close", last(col("Close")).over(window)) \
      .withColumn("Adj_Close", avg(col("Adj_Close")).over(window)) \
      .withColumn("Volume", sum(col("Volume")).over(window)) \
      .withColumn("Low", min(col("Low")).over(window)) \
      .withColumn("High", max(col("High")).over(window)) \
      .where(col("row")==1).select("Script_Name","Week_No","St_Date","End_Date","Open","Low","High","Close","Adj_Close","Volume")
    
    df_summary.show(5)
    
    #Write CSV to Outbound folder
    Outbound = "s3://"+bucketname+"/"+"pranav_nikam/Outbound/"
    print(Outbound + target_file_landing.split("/")[-1].split(".")[0]+"/"+"Output-"+target_file_landing.split("/")[-1].split(".")[0]+".csv")
    df_summary.toPandas().to_csv(Outbound + target_file_landing.split("/")[-1].split(".")[0]+"/"+"Output-"+target_file_landing.split("/")[-1].split(".")[0]+".csv")
    print("create_summary_file ended")
#df_summary.write.csv(Outbound + target_filename_landing.split("/")[-1].split(".")[0])


#object = s3.get_object(Bucket='bucketname',Key='pranav_nikam/Preprocess/INFY/Test_Results.txt')
#txt = (object['Body'].read().decode('utf-8'))

if __name__ == "__main__":
    bucketname = "saama-gene-training-data-bucket"
    #s3 = boto3.resource('s3')
    #my_bucket = s3.Bucket(bucketname)
    source = "pranav_nikam/Input_Infy/INFY"
    target = "pranav_nikam/Preprocess/INFY"
    
    #MOve to Landing folder
    source_preprocess = "pranav_nikam/Preprocess/INFY"
    target_landing = "pranav_nikam/Landing/INFY"
    
    CrawlerName = 'saama-gene-training-pranav-crawler-new'
    UserRole = 'saama-gene-training-glue-service-role'
    S3DatabaseName = 'saama-gene-training-data'
    regionName = 'ap-south-1'
    header_path = 'pranav_nikam/Preprocess/INFY/INFY_Headers.csv'
    counter_filepath = 'pranav_nikam/Preprocess/INFY/Counter_File_INFY.txt'
    #s3 = boto3.client("s3")
    s3_resource = boto3.resource('s3')
    my_bucket = s3_resource.Bucket(bucketname)
    
    target_file = move_file(bucketname,source,target,my_bucket)
    print(target_file)
    df = convert_xls_to_csv(bucketname, target, target_file)
    print(df)
    check_header_columns(my_bucket,header_path, df)
    check_counter_file(my_bucket,counter_filepath,df)
    landing_file_path = move_file(bucketname,source_preprocess,target_landing,my_bucket)
    print(landing_file_path)
    create_and_run_crawler(regionName,CrawlerName,UserRole,S3DatabaseName, landing_file_path)
    df_spark = create_parquet_file(bucketname, landing_file_path)
    create_summary_file(df_spark,landing_file_path)
    
        
    
