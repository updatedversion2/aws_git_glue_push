import boto3
import json

#loading secrets for retrieving db credentials
secret_arn = "arn:aws:secretsmanager:ap-south-1:941046533542:secret:postgre_Access-XqIQzh"
region_name = "ap-south-1"
session = boto3.session.Session()
sm_client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)
db_secret_response = sm_client.get_secret_value(
    SecretId=secret_arn
)
if 'SecretString' in db_secret_response:
    secret = db_secret_response['SecretString']
secret = json.loads(secret)
user = secret["username"]
password = secret["password"]


from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *


spark =SparkSession.builder.master("local[*]").appName("test").getOrCreate()
url="jdbc:postgresql://psdb.cc6eiigwukcm.ap-south-1.rds.amazonaws.com:5432/pdb"

df=spark.read.format("jdbc").option("url",url).option("user",user).option("password",password).option("driver","org.postgresql.Driver").option("dbtable","ADDRESS").load()
df.write.format("csv").option("header","true").option("mode","append").save("s3://aws23project/input/address1/")


