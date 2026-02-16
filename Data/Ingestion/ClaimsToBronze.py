from pyspark.sql import SparkSession, functions as f

#Initializing Spark Session
spark = SparkSession.builder.appName("ClaimsToBronze").getOrCreate()

#Cloud services configurations
GCS_BUCKET = "health-care-rcm-bucket-26jan029"
CLAIMS_DATA_PATH = f"gs://{GCS_BUCKET}/landing/claims/*.csv"
BQ_CLAIMS_TABLE = "project-b2d1202e-c674-4674-a8a.bronze_dataset.claims_data"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

#Reading claims data from GCS location
claims_df = (spark.read.format("csv")
            .option("header", True)
            .load(CLAIMS_DATA_PATH))


claims_df = (claims_df.withColumn("hospital_source",
                                 f.when(f.input_file_name().contains('hospital1'), "Hospital-A")
                                 .when(f.input_file_name().contains('hospital2'), "Hospital-B")
                                 .otherwise(None)
                                 )
             .dropDuplicates()
            )


#Writing to BQ Claims Table
(claims_df.write.format("bigquery")
                .option("table", BQ_CLAIMS_TABLE)
                .option("temporaryGcsBucket", BQ_TEMP_PATH)
                .mode("overwrite")
                .save())


