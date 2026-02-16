from pyspark.sql import SparkSession, functions as f


#Initializing Spark Session
spark = SparkSession.builder.appName("CptCodesToBronze").getOrCreate()

#Initialing Google services configurations
GCS_BUCKET = "health-care-rcm-bucket-26jan029"
CPT_PATH = f"gs://{GCS_BUCKET}/landing/cptcodes/*.csv"
BQ_CPT_TABLE = "project-b2d1202e-c674-4674-a8a.bronze_dataset.cpt_codes"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"


#Reading Cpt Codes data from Bucket location
cpt_df = (spark.read.format("csv")
                .option("header", True)
                .load(CPT_PATH))

newcol = [colm.replace(" ", "_") for colm in cpt_df.columns]

cpt_df = cpt_df.toDF(*newcol)

#Writing cpt codes to Bigquery
(cpt_df.write.format("bigquery")
            .option("table", BQ_CPT_TABLE)
            .option("temporaryGcsBucket", BQ_TEMP_PATH)
            .mode("overwrite")
            .save())


