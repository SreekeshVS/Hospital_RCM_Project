from google.cloud import storage, bigquery
import datetime
import json
import pandas as pd
from pyspark.sql import SparkSession


#Initializing Spark Session
spark = SparkSession.builder.appName("HospitalBtoLanding").getOrCreate()

#Initializing the google services clients
bqClient = bigquery.Client()
storageClient = storage.Client()

#GCS Bucket configuration
HOSPITAL_NAME = "hospital-b"
HOSPITAL_DB = "hospital_b_db"
GCS_BUCKET = "health-care-rcm-bucket-26jan029"
GCS_CONFIG_FILEPATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"
GCS_LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
GCS_ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"

#BigQuery Configuration
BQ_PROJECT = "project-b2d1202e-c674-4674-a8a"
BIGQUERY_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_logs"
BIGQUERY_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BIGQUERY_TEMP_LOCATION = f"{GCS_BUCKET}/temp/"

#MySQL configuration
mysql_config = {
    "url": f"jdbc:mysql://34.63.14.149:3306/{HOSPITAL_DB}",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "pass": "!Mypass123"
}

# List for capturing logs during the workflow
log_events = []

#logger function to capture logs
def log_event(logtype, logmessage, logtable=None):
    
    eventtime = datetime.datetime.now().isoformat()
    
    event_log = {
        "event_type": logtype,
        "event_message": logmessage,
        "event_time": eventtime,
        "event_table": logtable
    }
    log_events.append(event_log)
    print(f"[{eventtime}] - {logtype}: {logmessage}")


#Saving Pipeline logs to GCS Buckets
def save_logs_to_gcs_bucket():
    
    current_time = datetime.datetime.now().strftime("%Y%m%d%H%MI%SS")
    pipeline_file = f"/temp/pipeline_{current_time}.json"
    pipeline_blob = storageClient.bucket(GCS_BUCKET).blob(pipeline_file)
    
    if log_events:
        log_json = json.dumps(log_events, indent=4)
        
        pipeline_blob.upload_from_string(log_json, content_type="application/logs")
        
        print(f"Pipeline successfully placed at gs://{GCS_BUCKET}/{pipeline_file}")
        
def save_logs_to_bigquery():
    
    if log_events:
        
        pipeline_df = spark.createDataFrame(log_events)
        
        (pipeline_df.write.format("bigquery")
            .option("table", BIGQUERY_LOG_TABLE)
            .option("temporaryGcsBucket", BIGQUERY_TEMP_LOCATION)
            .mode("append")
            .save())
        
        print("Successfully loaded pipeline logs to bigquery for future analysis")


#Reading Config File
def get_config_file():
    
    df_in = spark.read.format("csv") \
        .option("header", True) \
        .load(GCS_CONFIG_FILEPATH)
        
    log_event("INFO", "Successfully read the config file")
    
    return df_in


#Archiving Existing files at the landing location
def archive_existing_landing_files(inptable):
    blobs = list(storageClient.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{inptable}"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]
    
    if not existing_files:
        log_event("INFO", f"No file to be archived at the landing zone for - {inptable}")
        
        
    for file in existing_files:
        file_name = file.split("_")[-1]
        date_part = file_name.split(".")[0]
        
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        
        source_blob = storageClient.bucket(GCS_BUCKET).blob(file)
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{inptable}/{year}/{month}/{day}/{file}"
        destination_blob = storageClient.bucket(GCS_BUCKET).blob(archive_path)
        
        storageClient.bucket(GCS_BUCKET).copy_blob(source_blob, storageClient.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()
        
        log_event("INFO", f"File : {file_name} successfully moved to Archive_Location  - {archive_path}", logtable=inptable)


#Function for getting latest watermark for the load table
def get_latest_watermark(inptable):
    
    inquery = f"""
        select max(loadtimestamp) as latest_timestamp
        from
            {BIGQUERY_AUDIT_TABLE}
        where
            tablename = '{inptable}'
        and
            datasource = '{HOSPITAL_DB}'
    """
    
    bqresult = bqClient.query(inquery)
    result = bqresult.result()
    
    for row in result:
        return row['latest_timestamp'] if row['latest_timestamp'] else "1900-01-01 00:00:00"
    
    return "1900-01-01 00:00:00"
        
        
#Extracting data from MySQL and loading to landing zone
def extract_and_load(loadtable, watermark_col, loadtype):
    
    try:
    
        latest_watermark = get_latest_watermark(loadtable) if loadtype.lower() == "incremental" else None
        log_event("INFO", f"Latest watermark for {loadtable}: {latest_watermark}", logtable=loadtable)
    
        inquery = f"(select * from {loadtable} where DeptID <> 'DeptID') as t" if loadtype.lower() == "full" else \
            f"(select * from {loadtable} where {watermark_col} > '{latest_watermark}' and cast({watermark_col} as CHAR) <> '0000-00-00') as t"
    
        df_extract = (spark.read.format("jdbc") 
            .option("url", mysql_config["url"]) 
            .option("driver", mysql_config["driver"]) 
            .option("user", mysql_config["user"]) 
            .option("password", mysql_config["pass"]) 
            .option("dbtable", inquery)
            .load())
    
        log_event("SUCCESS", f"Successfully read the data from table : {loadtable}", logtable=loadtable)
        
        #Writing the data to landing zone
        
        current_date = datetime.datetime.today().strftime("%d%m%Y")
        landing_file = f"landing/{HOSPITAL_NAME}/{loadtable}/{loadtable}_{current_date}.json"
        landing_blob = storageClient.bucket(GCS_BUCKET).blob(landing_file)
        
        landing_blob.upload_from_string(df_extract.toPandas().to_json(orient="records", lines=True), content_type="application/json")
        
        log_event("INFO", f"File - {loadtable} successfully placed at landing zone : gs://{GCS_BUCKET}/{landing_file}", logtable=loadtable)
        
        #Writing an Audit entry for successful load
        current_time = datetime.datetime.now()
        
        audit_entry = spark.createDataFrame(
        [(HOSPITAL_DB, loadtable, loadtype, df_extract.count(), current_time, "SUCCESS")],
            ["datasource", "tablename", "loadtype", "recordcount", "loadtimestamp", "status"])
        
        (audit_entry.write.format("bigquery") 
            .option("table", BIGQUERY_AUDIT_TABLE) 
            .option("temporaryGcsBucket", BIGQUERY_TEMP_LOCATION)
            .mode("append")
            .save())
        
        log_event("INFO", f"Successfully made Audit entry for table : {loadtable}", logtable=loadtable)
        
    except Exception as e:
        log_event("ERROR", f"Error while loading table : {loadtable}, {str(e)}", logtable=loadtable)
        


#Workflow for extracting, loading and archiving files for Hospital
config_df = get_config_file()

for row in config_df.collect():
    
    if row["is_active"] == '1' and row["datasource"] == HOSPITAL_DB:        
        database,datasource,tablename,loadtype,watermark,_,targetpath = row
        
        archive_existing_landing_files(tablename)
        extract_and_load(tablename, watermark, loadtype)
        
        
save_logs_to_gcs_bucket()
save_logs_to_bigquery()


