# 3 main files:
# - Raw
# - Ingested
# - Processed

# Enable / Disable Write Modes

enable_ingest = True # load which table?
enable_processed = True

enable_batch = False # load it how?
enable_stream = True

debug = False # (enables batch reads only)


# I/0 Paths

gcs_path_raw = "gs://datalake-flight-dev-1/flightsummary-ingest-raw-json"
gcs_path_ingested = 'gs://datalake-flight-dev-1/flightsummary-delta-ingested'
gcs_path_processed = 'gs://datalake-flight-dev-1/flightsummary-delta-processed'

# Derived paths
gcs_path_ingested_batch = f'{gcs_path_ingested}-batch'
gcs_path_ingested_stream = f'{gcs_path_ingested}-stream'

gcs_path_processed_batch = f'{gcs_path_processed}-batch'
gcs_path_processed_stream = f'{gcs_path_processed}-stream'

#############

partition_fields = ['crt_ts_year','crt_ts_month','crt_ts_day','crt_ts_hour']

#############

from datetime import datetime, timedelta
from google.cloud import storage
from IPython.display import display

import pandas as pd
pd.set_option("display.max_columns", 1000)

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

def pipe(self, func, *args, **kwargs):
    return func(self, *args, **kwargs)
DataFrame.pipe = pipe



spark = (SparkSession 
  .builder 
  .appName("sesh1") 
  .config("spark.master", "local[*]") 
  .config("spark.executor.memory", "1g") # 24g 
  .config("spark.driver.memory", "1g") # 6g
  .config("spark.default.parallelism", 1)
  .config("spark.sql.shuffle.partitions", 1)
  .config("spark.memory.fraction", 0.1)
  .config("spark.memory.storageFraction", 0.5)

  #.config("spark.driver.maxResultSize", "4g") 
  #.config("spark.debug.maxToStringFields",5000)
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") 
  .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.1") 
  .getOrCreate()
)

# spark.conf.set("spark.sql.repl.eagerEval.enabled",True)


schema_raw = StructType([
    StructField("ident", StringType(), True),
    StructField("ident_icao", StringType(), True),
    StructField("ident_iata", StringType(), True),
    StructField("fa_flight_id", StringType(), True),
    StructField("operator", StringType(), True),
    StructField("operator_icao", StringType(), True),
    StructField("operator_iata", StringType(), True),
    StructField("flight_number", StringType(), True),
    StructField("registration", StringType(), True),
    StructField("atc_ident", StringType(), True),
    StructField("inbound_fa_flight_id", StringType(), True),
    StructField("codeshares", StringType(), True),
    StructField("codeshares_iata", StringType(), True),
    StructField("blocked", BooleanType(), True),
    StructField("diverted", BooleanType(), True),
    StructField("cancelled", BooleanType(), True),
    StructField("position_only", BooleanType(), True),
    StructField("departure_delay", LongType(), True),
    StructField("arrival_delay", LongType(), True),
    StructField("filed_ete", LongType(), True),
    StructField("foresight_predictions_available", BooleanType(), True),
    StructField("scheduled_out", TimestampType(), True),
    StructField("estimated_out", TimestampType(), True),
    StructField("actual_out", TimestampType(), True),
    StructField("scheduled_off", TimestampType(), True),
    StructField("estimated_off", TimestampType(), True),
    StructField("actual_off", TimestampType(), True),
    StructField("scheduled_on", TimestampType(), True),
    StructField("estimated_on", TimestampType(), True),
    StructField("actual_on", TimestampType(), True),
    StructField("scheduled_in", TimestampType(), True),
    StructField("estimated_in", TimestampType(), True),
    StructField("actual_in", TimestampType(), True),
    StructField("progress_percent", LongType(), True),
    StructField("status", StringType(), True),
    StructField("aircraft_type", StringType(), True),
    StructField("route_distance", LongType(), True),
    StructField("filed_airspeed", LongType(), True),
    StructField("filed_altitude", DoubleType(), True),
    StructField("route", StringType(), True),
    StructField("baggage_claim", StringType(), True),
    StructField("seats_cabin_business", StringType(), True),
    StructField("seats_cabin_coach", StringType(), True),
    StructField("seats_cabin_first", StringType(), True),
    StructField("gate_origin", StringType(), True),
    StructField("gate_destination", StringType(), True),
    StructField("terminal_origin", StringType(), True),
    StructField("terminal_destination", StringType(), True),
    StructField("type", StringType(), True),
    StructField("origin_code", StringType(), True),
    StructField("origin_code_icao", StringType(), True),
    StructField("origin_code_iata", StringType(), True),
    StructField("origin_code_lid", StringType(), True),
    StructField("origin_timezone", StringType(), True),
    StructField("origin_name", StringType(), True),
    StructField("origin_city", StringType(), True),
    StructField("origin_airport_info_url", StringType(), True),
    StructField("destination_code", StringType(), True),
    StructField("destination_code_icao", StringType(), True),
    StructField("destination_code_iata", StringType(), True),
    StructField("destination_code_lid", StringType(), True),
    StructField("destination_timezone", StringType(), True),
    StructField("destination_name", StringType(), True),
    StructField("destination_city", StringType(), True),
    StructField("destination_airport_info_url", StringType(), True),
    StructField("crt_ts", LongType(), True),
    StructField("crt_ts_year", LongType(), True),
    StructField("crt_ts_month", LongType(), True),
    StructField("crt_ts_day", LongType(), True),
    StructField("crt_ts_hour", LongType(), True),
    StructField("last_run_ts", LongType(), True),
    StructField("last_scheduled_out_ts", LongType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True)
])

############################ INGESTION LOGIC ############################

def ingest_data_processing(df):
    
    def add_ingest_timestamp_utc(df):
        # Add ingestion timestamp to dataframe
        current_time_utc = from_utc_timestamp(current_timestamp(), "UTC")
        return df.withColumn("ingest_ts", current_time_utc)
    
    def cast_unix_to_timestamp_types(df):
        # Cast Unix to Timestamp Types
        date_format = "yyyy-MM-dd HH:mm:ss"
        return (
            df.withColumn('crt_ts', from_unixtime(col("crt_ts")/1000 , date_format))
              .withColumn('last_run_ts', from_unixtime(col("last_run_ts")/1000 , date_format))
              .withColumn('last_scheduled_out_ts', from_unixtime(col("last_scheduled_out_ts")/1000 , date_format))
        )
        
    df = cast_unix_to_timestamp_types(df)
    df = add_ingest_timestamp_utc(df)
    df = df.drop(*['year','month','day'])    
    return df

############################ INGESTION ############################

if enable_ingest: # loads raw JSONs to Delta Table
    
    if enable_batch:

        df_raw = (
            spark.read
                 .option("encoding", "UTF-8")
                 .option("multiLine", True)
                 .schema(schema_raw)
                 .json(gcs_path_raw)

            .pipe(ingest_data_processing) # processing here
        )

        if debug:
            display(df_raw.limit(10000).toPandas())
        else: 
            (
            df_raw
                .write
                .format("delta")
                .mode("overwrite")
                .partitionBy(*partition_fields)
                .save(gcs_path_ingested_batch)
            )
            print(f'batch written to {gcs_path_ingested_batch}')


    if enable_stream and not debug:
        df_raw = (
            spark.readStream
                 .schema(schema_raw)
                 .option("encoding", "UTF-8")
                 .option("multiLine", True)
                 .option("maxFilesPerTrigger", 1)
                 .json(gcs_path_raw)

            .pipe(ingest_data_processing) # processing here
        )

        streaming_query_ingest = (
            df_raw.writeStream \
                       .format("delta")
                       .partitionBy(*partition_fields)
                       .option("checkpointLocation", f'{gcs_path_ingested_stream}-checkpoint')
                       .outputMode("append")
                       .start(gcs_path_ingested_stream)
        )
        print(f'streaming to {gcs_path_ingested_stream}')
        # streaming_query_ingest.awaitTermination()
        
############################ PRE-PROCESSING LOGIC ############################

def flights_unpivot_transformation(df):
    df.createOrReplaceTempView('flights_ingested')
    
    unpivot_query =  """
    CREATE OR REPLACE TEMPORARY VIEW flights_processed AS (
      SELECT 
        *
      FROM (
        SELECT 
          *,
          stack(5, 'scheduled_out', scheduled_out, 'actual_out', actual_out, 'actual_off', actual_off, 'actual_on', actual_on, 'actual_in', actual_in) 
          AS (event_type, event_ts)
        FROM 
          flights_ingested
      )
      WHERE 
        (event_ts BETWEEN last_run_ts AND crt_ts AND event_type != 'scheduled_out')
        OR 
        (event_type = 'scheduled_out' AND (scheduled_out IS DISTINCT FROM last_scheduled_out_ts OR last_scheduled_out_ts IS NULL))
      -- ORDER BY 
        -- fa_flight_id DESC, crt_ts DESC
    );

    """
    spark.sql(unpivot_query)
    return spark.sql('select * from flights_processed;')

def flights_processing(df, streaming=False):
    
    df = flights_unpivot_transformation(df)

    if not streaming:
        df = df.orderBy(col("fa_flight_id").desc(), col("crt_ts").desc())
    
    return df
        

############################ PRE-PROCESSING ############################

if enable_processed: # loads raw JSONs to Delta Table
    
    if enable_batch:

        df_proc = (
            spark.read
                 .format("delta")
                 .load(gcs_path_ingested_batch)

            .pipe(flights_processing) # processing here
        )

        if debug:
            display(df_proc.limit(10000).toPandas())
        else: 
            (
            df_proc
                .write
                .format("delta")
                .mode("overwrite")
                .partitionBy(*partition_fields)
                .save(gcs_path_processed_batch)
            )
            print(f'batch written to {gcs_path_processed_batch}')


    if enable_stream and not debug:
        df_proc = (
            spark.readStream
                 .format("delta")
                 .option("maxFilesPerTrigger", 1) \
                 .load(gcs_path_ingested_stream)

            .pipe(flights_processing, streaming=True) # processing here
        )

        streaming_query_proc = (
            df_proc.writeStream \
                       .format("delta")
                       .partitionBy(*partition_fields)
                       .option("checkpointLocation", f'{gcs_path_processed_stream}-checkpoint')
                       .outputMode("append")
                       .start(gcs_path_processed_stream)
        )
        print(f'streaming to {gcs_path_processed_stream}')
        # streaming_query_proc.awaitTermination()
        
if enable_stream:
    if enable_ingest:
        streaming_query_ingest.awaitTermination()
    if enable_processed:
        streaming_query_proc.awaitTermination()


