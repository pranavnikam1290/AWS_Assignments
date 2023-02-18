import sys
import boto3
import boto3
import io
import pandas as pd
import pyspark
#import dask.dataframe as dd

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from botocore.exceptions import ClientError
from pyspark.sql.functions import count
from zipfile  import ZipFile

from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
#Create PySpark SparkSession
#spark = SparkSession.builder.appName("Session_1").getOrCreate()


bucket = 'saama-gene-training-data-bucket'
inbound = 'pranav_nikam/Use_Case_2/Inbound/'
landing = 'pranav_nikam/Use_Case_2/Landing/'
temp_dir = 'pranav_nikam/Use_Case_2/Temp/'

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)



s3 = boto3.client("s3")

bucket = 'saama-gene-training-data-bucket' # your s3 bucket name
prefix = 'pranav_nikam/Use_Case_2/Inbound/' # the prefix for the objects that you want to unzip
unzip_prefix = 'pranav_nikam/Use_Case_2/Temp/' # the location where you want to store your unzipped files 

# Get a list of all the resources in the specified prefix
objects = s3.list_objects(
    Bucket=bucket,
    Prefix=prefix
)["Contents"]

# The following will get the unzipped files so the job doesn't try to unzip a file that is already unzipped on every run
unzipped_objects = s3.list_objects(
    Bucket=bucket,
    Prefix=unzip_prefix
)["Contents"]

# Get a list containing the keys of the objects to unzip
object_keys = [ o["Key"] for o in objects if o["Key"].endswith(".zip") ] 
# Get the keys for the unzipped objects
unzipped_object_keys = [ o["Key"] for o in unzipped_objects ] 

for key in object_keys:
    obj = s3.get_object(
        Bucket= bucket,
        Key=key
    )
    
    objbuffer = io.BytesIO(obj["Body"].read())
    
    # using context manager so you don't have to worry about manually closing the file
    with ZipFile(objbuffer) as zip:
        filenames = zip.namelist()

        # iterate over every file inside the zip
        for filename in filenames:
            with zip.open(filename) as file:
                filepath = unzip_prefix + filename
                if filepath not in unzipped_object_keys:
                    s3.upload_fileobj(file, bucket, filepath)

s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucket)

#Convert csv.gz to csv file
target_file = "s3://"+bucket+"/"+temp_dir+"INFY_202101.csv.gz"

source = 'pranav_nikam/Use_Case_2/Temp'
target = 'pranav_nikam/Use_Case_2/Landing'

foldername_list = set()
for obj in my_bucket.objects.filter(Prefix = source):
    source_filename = (obj.key).split('/')[-1]
    copy_source = {
        'Bucket': bucket,
        'Key': obj.key
    }
    target_filename = "{}/{}".format(target,source_filename)
    print(target_filename)
    if target_filename.endswith('.csv.gz'):
        print("True")
        read_file = pd.read_csv("s3://"+bucket+"/"+source+"/"+source_filename, compression='gzip', header=0, sep=',', quotechar='"')
        
        #Convert dataframe columns from String to Double type
        #Create PySpark DataFrame from Pandas
        read_file_spark= spark.createDataFrame(read_file)
        '''
        read_file['Open'] = read_file['Open'].astype(float)
        read_file['High'] = read_file['High'].astype(float)
        read_file['Low'] = read_file['Low'].astype(float)
        read_file['Close'] = read_file['Close'].astype(float)
        #read_file['Adj Close'] = read_file['Adj Close'].astype(float)
        #read_file['Volume'] = read_file['Volume'].astype(float)
        '''
        read_file_spark = read_file_spark.withColumnRenamed("Adj Close", "Adj_Close") \
        .withColumnRenamed("Script name", "Script_name")
   
        #Convert Column format from String to Double
        read_file_spark = read_file_spark.withColumn("Open",col("Open").cast((DoubleType()))).withColumn("High",col("High").cast((DoubleType()))).withColumn("Low",col("Low").cast((DoubleType()))).withColumn("Close",col("Close").cast((DoubleType())))

        
        read_file = read_file_spark.toPandas()
        
        read_file.to_csv("s3://"+bucket+"/"+target+"/"+source_filename.split('/')[-1].split('.')[0].split('_')[0]+"/"+source_filename.split('/')[-1].split('.')[0].split('_')[1]+"/"+source_filename.split('/')[-1].split('.')[0].split('_')[0]+"-"+source_filename.split('/')[-1].split('.')[0].split('_')[1]+"/"+"saama-gene-training-pranav-"+source_filename.split('/')[-1].split('.')[0]+".csv", index = None,header=True,sep=",")
        
        foldername_list.add(source_filename.split('/')[-1].split('.')[0].split('_')[0])

#Create and Run crawler function        
def create_and_run_crawler(regionName,CrawlerName,UserRole,S3DatabaseName, path):
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
            Name = CrawlerName,
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
            Name = CrawlerName
        )
        
#Create Athena table for each Source
for x in foldername_list:
    objects = list(s3.Bucket(bucket).objects.filter(Prefix=landing + x +'/'))
    objects.sort(key=lambda o: o.last_modified)
    print(objects[-1].key)
    CrawlerName = 'saama-gene-training-pranav-crawler-'+x+"-"+objects[-1].key.split("_")[-1].split(".")[0]
    print(CrawlerName)
    UserRole = 'saama-gene-training-glue-service-role'
    S3DatabaseName = 'saama-gene-training-data'
    regionName = 'ap-south-1'
    path = "s3://"+bucket+"/"+landing+x+"/"+objects[-1].key.split("_")[-1].split(".")[0]+"/"+x+"-"+objects[-1].key.split("_")[-1].split(".")[0]+"/"
    print("craler path:", path)
    create_and_run_crawler(regionName,CrawlerName,UserRole,S3DatabaseName, path)


#Move old files to Archive folder
for x in foldername_list:
    objects = list(s3.Bucket(bucket).objects.filter(Prefix=landing + x +'/'))
    objects.sort(key=lambda o: o.last_modified)
    for i in range(len(objects) - 1):
        source_filename = (objects[i].key).split('/')[-1]
        print(source_filename)
        copy_source = {
            'Bucket': bucket,
            'Key': objects[i].key
        }
        target_filename_archive = "{}{}".format(temp_dir,source_filename)
        s3.meta.client.copy(copy_source, bucket, target_filename_archive)
        s3.Object(bucket, objects[i].key).delete() 


    


#job.commit()

#if __name__ == "__main__":
    
    # Path variables

    
    # ETL Process
   # unzip_files_to_temp(bucket, inbound, temp_dir)
   # temp_to_landing(bucket, temp_dir, landing)

#job.commit()