#Import libraries
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws, from_unixtime, dense_rank, monotonically_increasing_id
from pyspark.sql.functions import hour, minute,  dayofmonth, month, year
from pyspark.sql.window import Window
import psycopg2
import argparse


#Initialing Spark Session
spark = SparkSession.builder.getOrCreate()

# Setting variables
parser = argparse.ArgumentParser()
# Receiving variables
parser.add_argument('--Access_Key',help='User access key')
parser.add_argument('--Secret_Key',  help='Secret access key')
parser.add_argument('--Bucket',  help='Bucket where the data is')
parser.add_argument('--Prefix',  help='Prefix in the Bucket where the data is')
parser.add_argument('--Host',  help='AWS Redshift Host')
parser.add_argument('--Port',  help='AWS Redshift Port')
parser.add_argument('--User',  help='AWS Redshift User')
parser.add_argument('--Password',  help='AWS Redshift Password')
parser.add_argument('--Database',  help='AWS Redshift Database')

args = vars(parser.parse_args())

#Global Variables
Access_Key = args['Access_Key']
Secret_Key = args['Secret_Key']

#Bucket
Bucket = args['Bucket']
Prefix = args['Prefix']
S3_Uri = f's3://{Bucket}/{Prefix}'

#Redshift variables
host =args['Host']
port = args['Port']
database = args['Database']
user = args['User']
password = args['Password']


# Setting AWS Credentials in  code
session = boto3.Session(
    aws_access_key_id=Access_Key,
    aws_secret_access_key=Secret_Key
)
credentials = session.get_credentials()
spark._jsc.hadoopConfiguration().set(
    'fs.s3a.access.key', credentials.access_key)
spark._jsc.hadoopConfiguration().set(
    'fs.s3a.secret.key', credentials.secret_key)

#Review about the items in the bucket with the prefix
try:
    s3_client = session.client('s3')

    response = s3_client.list_objects_v2(
        Bucket=Bucket,
        Prefix=Prefix
    )
except Exception as e:
    print(f"Error: {e}")

def Reading_Data():
    # # Reading data
    print('Reading dataset')
    ds = spark.read.json("data-sample.json")

    #The credentials doesn't work, so I decided to leave evidence about how to read data from a Bucket in json or parquet format
    #json_df = spark.read.json(S3_Uri)
    #parquet_df = spark.read.parquet(S3_Uri)
    return ds

def ETLProcess(ds):
    print('ETL Process')
# # ETL Process
    ds = ds.drop('_corrupt_record')
    ds = ds.withColumnRenamed('id', 'id_principal')
    # Splitting columns
    ds = ds.select("adverts.*", 
                explode("applicants").alias("applicant"), 
                "benefits", 
                "city", 
                "company", 
                "id_principal", 
                "sector", 
                "title")

    # Splitt "applicant" columns
    ds = ds.select("activeDays", 
                "applyUrl", 
                "id", 
                "publicationDateTime", 
                "status",
                col("applicant.age").alias("age"),
                col("applicant.applicationDate").alias("applicationDate"),
                col("applicant.firstName").alias("firstName"),
                col("applicant.lastName").alias("lastName"),
                col("applicant.skills").alias("skills"),
                "benefits", 
                "city", 
                "company", 
                "id_principal", 
                "sector", 
                "title")

    #Splitting array columns
    ds = ds.withColumn("benefits_split", concat_ws(", ", "benefits"))
    ds = ds.withColumn("skills_split", concat_ws(", ", "skills"))

    #Dropping array columns
    Columns_to_drop = ["skills", "benefits"]
    ds = ds.drop(*Columns_to_drop)
    ds = ds.withColumnRenamed('id', 'id_adverts')
    # # Ordering ds
    ds = ds.select("id_principal",
                "id_adverts",
                    "company",
                "city",
                "sector",
                "title",
                "status",
                "publicationDateTime",
                "activeDays",
                "applyUrl",
                "benefits_split",
                "firstName",
                "lastName",
                "age",
                "skills_split" ,
                "applicationDate"
                )
    # # Casting date colums
    ds = ds.withColumn("publicationDateTime", from_unixtime(col("publicationDateTime")).cast("timestamp"))
    ds = ds.withColumn("applicationDate", from_unixtime(col("applicationDate")).cast("timestamp"))
    df = ds.dropna(subset=["firstName"])
    return df

def CreatingDim(df):
    print('Creating dimensional model')
    # # Analysis
    # #In order to have the table facts and dimensions, I recommend:
    # #Facts: Application
    # #Dimensions: company, date, work, candidate

    # # Dimension

    #Creating ID per dimensions 
    df = df.withColumn("ID_Company", dense_rank().over(Window.orderBy("company")))
    df = df.withColumn("ID_Date", dense_rank().over(Window.orderBy("publicationDateTime")))

    #For candidate first of all I'm going to create the full name column to create and id based on that column
    df = df.withColumn("Full_Name", concat_ws(" ",df.firstName, df.lastName))
    df = df.withColumn("ID_Candidate", dense_rank().over(Window.orderBy("Full_Name")))

    #Company
    D_Company = df.select('ID_Company', 'company','city','sector').distinct()

    #Date
    D_Date = df.select('ID_Date', 'publicationDateTime', 'activeDays').dropDuplicates()

    #Extracting day, month and year trnasformations
    D_Date = D_Date.withColumn("_day", dayofmonth(D_Date["publicationDateTime"]))\
            .withColumn("_month", month(D_Date["publicationDateTime"]))\
            .withColumn("_year", year(D_Date["publicationDateTime"]))\
            .withColumn("_hour", hour(D_Date["publicationDateTime"]))\
            .withColumn("_minute", minute(D_Date["publicationDateTime"]))


    #Candidate
    D_Candidate = df.select('ID_Candidate', 'Full_Name', 'firstName','lastName','age','skills_split','applicationDate').dropDuplicates()


    #Work
    D_Work = df.select('id_adverts', 'title', 'status', 'applyUrl', 'benefits_split').dropDuplicates()

    # # Facts Table

    Fact_Table = df.select("id_principal",
                        "ID_Company",
                        "ID_Date",
                        "id_adverts",
                        "ID_Candidate"
                        )
    return (D_Work, D_Date, D_Company, D_Candidate, Fact_Table)

def InsertIntoRedshift(df, Table_Name):
    #Creating connection
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password
    )
    cursor = conn.cursor()
    #Assuming the tables already exists
    #Creating Temp df
    table_name_temp = f'{Table_Name}_temp'
    df.createOrReplaceTempView(table_name_temp)

    #Inserting dataframe/table. In this part I assumed the tables already exists
    insert_query = f'''
        INSERT INTO Table_Name{}
        SELECT *
        FROM {table_name_temp}
    '''
    cursor.execute(insert_query)
    conn.commit()

    #Closing connection
    cursor.close()
    conn.close()
if __name__ == '__main__':
    print('Begin Pipeline')
    df = Reading_Data()

    df = ETLProcess(df)

    D_Work, D_Date, D_Company, D_Candidate, Fact_Table = CreatingDim(df)

    InsertIntoRedshift(D_Company, 'DIM_COMPANY')
    InsertIntoRedshift(D_Work, 'DIM_WORK')
    InsertIntoRedshift(D_Date, 'DIM_DATE')
    InsertIntoRedshift(D_Candidate, 'DIM_CANDIDATE')
    InsertIntoRedshift(Fact_Table, 'FACT_TABLE')

    print('Finis Pipeline')