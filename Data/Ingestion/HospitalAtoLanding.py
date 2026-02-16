from google.cloud import storage, bigquery
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json

#Initialize Spark session
spark = SparkSession.builder \
    .appName("HospitalAtoLanding") \
    .getOrCreate()

#Initializing google clients
storageClient = storage.Client()
bqClient = bigquery.Client()

#Initialize GCS configuration
GCS_BUCKET = "health-care-rcm-bucket-26jan029"
HOSPITAL_NAME = "hospital-a"
HOSPITAL_DB = "hospital_a_db"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"


#BigQuery Configuration
BQ_PROJECT = "project-b2d1202e-c674-4674-a8a"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_logs"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_LOCATION = f"{GCS_BUCKET}/temp/"

#MySQL Datasource configuration
mysql_config = {
    "url": f"jdbc:mysql://34.61.116.189:3306/{HOSPITAL_DB}",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "pass": "!Mypass123"
}


# Initializing logger datastructure
logs_list = []


def log_event(log_type, log_message, log_table = None):
    '''
    Capturing logs across the ingestion process and pushing it to the logs list
    
    '''
    log_entry = {
        "event_time": datetime.datetime.now().isoformat(),
        "event_type": log_type,
        "event_message": log_message,
        "event_table": log_table
    }
    logs_list.append(log_entry)
    #printing logs on console
    print(f"[{log_entry['event_time']}] - {log_type} : {log_message}")
    

def save_logs_to_bucket():
    """
    Saving pipeline logs to GCS bucket post the completion of the workflow
    """
    
    logfile_name = f"pipeline_logs_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    logfile_path = f"temp/pipeline_logs/{logfile_name}"
    
    json_data = json.dumps(logs_list, indent=4)
    
    #Defined GCS location
    bucket = storageClient.bucket(GCS_BUCKET)
    blob= bucket.blob(logfile_path)
    
    #Upload json pipeline logs to the bucket
    blob.upload_from_string(json_data, content_type="application/logs")
    
    print(f"Logs successfully uploaded to the GCS temp location : gs://{GCS_BUCKET}/{logfile_path}")
    
    
def save_logs_to_bigquery():
    """
    Saving pipeline logs to bigquery for audit purposes
    """
    
    if logs_list:
        pipeline_df = spark.createDataFrame(logs_list)
        
        (pipeline_df.write.format("bigquery")
            .option("table", BQ_LOG_TABLE)
            .option("temporaryGcsBucket", BQ_TEMP_LOCATION)
            .mode("append")
            .save())
        
        print(f"Logs stored in BigQuery for future analysis")
        

    

def read_config_file():
    df_in = spark.read.csv(CONFIG_FILE_PATH, header = True)
    log_event("INFO", "Successfully read config file")
    
    return df_in


def archive_existing_files(archive_tablename):
    blobs = list(storageClient.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{archive_tablename}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]
    
    if not existing_files:
        log_event("INFO", f"No existing files to archive for table - {archive_tablename}")
        
    for file in existing_files:
        source_blob = storageClient.bucket(GCS_BUCKET).blob(file)
        
        #Extract date parts from the file
        file_name = file.split("_")[-1]
        date_part = file_name.split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        
        #Move to archive
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{archive_tablename}/{year}/{month}/{day}/{file}"
        destination_blob = storageClient.bucket(GCS_BUCKET).blob(archive_path)
        
        #Copy files to archive location
        
        storageClient.bucket(GCS_BUCKET).copy_blob(source_blob, storageClient.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()
        
        log_event("INFO", f"File - {file} moved to archive location - {archive_path}", log_table=archive_tablename)


#Get latest watermark for the table to be loaded       
def get_latest_watermark(load_table):
    
    inquery = f"""
        select 
            max(loadtimestamp) as latest_timestamp
        from
            {BQ_AUDIT_TABLE}
        where
            tablename = '{load_table}'
        and 
            datasource = '{HOSPITAL_DB}'
        
    """
    query_job = bqClient.query(inquery)
    result = query_job.result()
    
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    
    return "1900-01-01 00:00:00"


#Read the source data from the MySQL DB and place the extracted data into landing zone
def extract_and_target_ingestion(load_table, watermark_col, load_type):
    
    try:
        last_watermark = get_latest_watermark(load_table) if load_type.lower() == "incremental" else None
        log_event("INFO", f"Latest watermark for {load_table}: {last_watermark}", log_table=load_table)
        
        inquery = f"(select * from {load_table} where DeptID <> 'DeptID') as t" if load_type.lower() == "full" \
            else f"(select * from {load_table} where {watermark_col} > '{last_watermark}' and cast({watermark_col} as CHAR) <> '0000-00-00' ) as t"
        
        
        spark.sql("LIST JARS").show()
        
        df_extract = (spark.read.format("jdbc")
            .option("url", mysql_config["url"])
            .option("driver", mysql_config["driver"])
            .option("user", mysql_config["user"])
            .option("password", mysql_config["pass"])
            .option("dbtable", inquery)
            .load())
        
        log_event("SUCCESS", f"Successfully extracted data for table - {load_table}", log_table=load_table)
        
        timenow = datetime.datetime.today().strftime("%d%m%Y")
        JSON_FILE_PATH = f"landing/{HOSPITAL_NAME}/{load_table}/{load_table}_{timenow}.json"
        
        landing_bucket = storageClient.bucket(GCS_BUCKET)
        landing_blob = landing_bucket.blob(JSON_FILE_PATH)
        landing_blob.upload_from_string(df_extract.toPandas().to_json(orient="records", lines=True), content_type="application/json")
        
        log_event("SUCCESS", f"JSON file successfully written to gs://{GCS_BUCKET}/{JSON_FILE_PATH}", log_table=load_table)

        
        #Audit entry
        audit_df = spark.createDataFrame([
            (HOSPITAL_DB, load_table, load_type, df_extract.count(), datetime.datetime.now(), "SUCCESS" )
        ],
            ["datasource", "tablename", "loadtype", "recordcount", "loadtimestamp", "status"]
                                        )
        
        (audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .mode("append")
            .save())
        
        log_event("SUCCESS", f"Audit log updated from table - {load_table}", log_table=load_table)
    
    except Exception as e:
        log_event("ERROR", f"Error processing table - {load_table}: {str(e)}", log_table=load_table)
        
        
        
# Data Ingestion Workflow

config_df = read_config_file()


for row in config_df.collect():
    
    if row["is_active"] == '1' and row["datasource"] == HOSPITAL_DB:
        
        database, datasource, tablename, loadtype, watermark, _, targetpath = row
        archive_existing_files(tablename)
        extract_and_target_ingestion(tablename, watermark, loadtype)
        

save_logs_to_bucket()
save_logs_to_bigquery()
