import sys
import requests
import json

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F  
from datetime import date,datetime, timedelta
import pyathena
import boto3 
import pandas as pd

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, ['JOB_NAME','customer_name','domain_name'])

failureEmailList = ''
successEmailList = '' + ',' + failureEmailList


today = date.today() - timedelta(days=0)
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")

#msck repair table temptest.de_source_rosieapp;
print("flattening for ", today)

#######################################__SEND MESSAGE TO SLACK__#############################################################

##Updated module code 110123 - Sanket
def slack_webhook():
    secret_name = ""
    region_name = ""
    
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    slack = client.get_secret_value(SecretId=secret_name)['SecretString']
    
    slack_webhook=json.loads(slack)
   
    return slack_webhook



def send_message_to_slack(channel, username, message):
    
    slack_web=slack_webhook()
    url = slack_web['slack_webhook']
    payload={"channel": channel, "username": username, "text": message}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
    except:
        pass
    return None

##########################################__SECRET MANAGER ACCESS KEYS__######################################################

secret_name = ""
region_name = ""

session = boto3.session.Session()
client = session.client(service_name='secretsmanager', region_name=region_name)

aws_access = client.get_secret_value(SecretId=secret_name)['SecretString']

# #print(aws_access)
aws_access=json.loads(aws_access)
#print(type(aws_access))


################################################################################################################################

athena_conn = pyathena.connect(aws_access_key_id=aws_access['aws_access_key_id'],
                          aws_secret_access_key=aws_access['aws_secret_access_key'],
                                   s3_staging_dir= 's3://testing-0255/temp/',
                          region_name='us-east-1')
                      

s3_client = boto3.client('s3')

customer_name=args['customer_name']
domain_name=args['domain_name']

query ="""select * from de_shipt_crawled_report.customer_control_table where lower(customer)=lower('{}') and lower(domain)=lower('{}')""".format(customer_name, domain_name)

df = pd.read_sql(query, athena_conn)

domain_data = df.to_dict(orient='records')

rawdata_s3_location=domain_data[0].get('rawdata_s3_location')

df=spark.read.json('{}year={}/month={}/day={}/*/*'.format(rawdata_s3_location,year,month,day)) 

# df=spark.read.json('s3://bungee.competitiveintelligence.reports/rosieappGroceryProducts/year={}/month={}/day={}/*/*'.format(year,month,day)) 

# s3://bungee.competitiveintelligence.reports/starmarketGroceryProducts/year={}/month={}/day={}/*/*'.format(year,month,day)

#df= spark.sql('SELECT * FROM temptest.de_zipcode_19013')

count_before= df.select(F.countDistinct("id")).toPandas().values[0][0]

# print(df.count())

# print("id distinct count" , count_before)

#print("id distinct count" , df.select(F.countDistinct("attributes_store_sku")).show())


def flatten_df(nested_df):
    #identify & segregate flattened, array & struct type columns
    flat_cols = [c[0] for c in nested_df.dtypes if  not 'struct' in c[1][:6] ]
    nested_cols = [c[0] for c in nested_df.dtypes if  'struct' in c[1][:6] ]
    array_cols= [c[0] for c in nested_df.dtypes if  'array' in c[1][:6] ]
    
    nested_df.printSchema();
    
    print(flat_cols)
    print(nested_cols)
    
    #flatten the nested aka structure type columns by iterating over the list of columns 
    #append the already flatten columns to the struct flattend columns
    flat_df = nested_df.select(flat_cols +
                                   [F.col(nc+'.'+c).alias(nc+'_'+c) for nc in nested_cols for c in nested_df.select(nc+'.*').columns])

    #flatten the array type columns using explode function from spark sql functions
    for i in array_cols:
        flat_df=flat_df.withColumn(i, F.explode_outer(df[i]))
    
    return flat_df

try:
    send_message_to_slack('#de-etlops-alerts', 'flatten_json_data_shipt -> '+domain_name, 'Job Started\nJob Type : PROD')
    
    df=flatten_df(df)
    df=flatten_df(df)
    df=flatten_df(df)
    df=flatten_df(df)
    df=flatten_df(df)
    df=flatten_df(df)
    df=flatten_df(df)

except:
    pass

count_after= df.select(F.countDistinct("id")).toPandas().values[0][0]


print("id distinct count flatten " , count_after)

#print("id distinct count flatten" , df.select(F.countDistinct("attributes_store_sku")).show())

#df_p=df.toPandas()

#df_p = df_p.astype(str)

#df = spark.createDataFrame(df_p) 
domain_name_l = domain_name.lower()
df.repartition(1).write.parquet('s3://bungee.customeringestion.data/prod/source={}/year={}/month={}/day={}/'.format(domain_name_l,year,month,day))

#s3://bungee.customeringestion.data/prod/source=starmarket/year={}/month={}/day={}/'.format(year,month,day)

msck_query ="""MSCK REPAIR TABLE de_shipt_crawled_report.de_source_{}""".format(domain_name_l)

df = pd.read_sql(msck_query, athena_conn)

job.commit()

send_message_to_slack('#de-etlops-alerts', 'flatten_json_data_shipt -> '+domain_name, 'Job Completed.\nDate : {} \nCount Before Flatten {}\nCount After Flatten {}'.format(str(today),count_before,count_after))


