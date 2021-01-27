# The following data processing script uses Spark to read in the Hive table that
# was created in 1_data_ingest.py and sample records from it to balance out the
# cancelled and not-cancelled classes. This sampled dataset is then saved to the local
# project to be used for modeling in the 5_model_train.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if os.environ["STORAGE_MODE"] == "local":
    sys.exit(
        "Skipping 1_data_ingest.py because excution is limited to local storage only."
    )

spark = (
    SparkSession.builder.appName("Airline Data Exploration")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.cores", "4")
    .config("spark.driver.memory", "20g")
    .config("spark.executor.instances", "4")
    .config("spark.yarn.access.hadoopFileSystems", os.environ["STORAGE"])
    .getOrCreate()
)

# Lets query the table from Hive
hive_database = os.environ["HIVE_DATABASE"]
hive_table = os.environ["HIVE_TABLE"]
hive_table_fq = hive_database + "." + hive_table

flight_df = spark.sql(f"select * from {hive_table_fq}")

flight_df.persist()
flight_df.printSchema()

print(f"There are {flight_df.count()} records in {hive_table_fq}.")

# Since majority of flights are not cancelled, lets create a more balanced dataset
# by undersampling from non-cancelled flights

sample_normal_flights = flight_df.filter("CANCELLED == 0").sample(
    withReplacement=False, fraction=0.03, seed=3
)

cancelled_flights = flight_df.filter("CANCELLED == 1")

all_flight_data = cancelled_flights.union(sample_normal_flights)
all_flight_data.persist()
# all_flight_data = all_flight_data.withColumn("date",to_date(concat_ws("-","year","month","dayofmonth"))).withColumn("week",weekofyear("date"))

all_flight_data = all_flight_data.withColumn(
    "HOUR",
    substring(
        when(length(col("CRS_DEP_TIME")) == 4, col("CRS_DEP_TIME")).otherwise(
            concat(lit("0"), col("CRS_DEP_TIME"))
        ),
        1,
        2,
    ).cast("integer"),
).withColumn("WEEK", weekofyear("FL_DATE"))

smaller_all_flight_data = all_flight_data.select(
    "FL_DATE",
    "OP_CARRIER",
    "OP_CARRIER_FL_NUM",
    "ORIGIN",
    "DEST",
    "CRS_DEP_TIME",
    "CRS_ARR_TIME",
    "CANCELLED",
    "CRS_ELAPSED_TIME",
    "DISTANCE",
    "HOUR",
    "WEEK",
)

smaller_all_flight_data.printSchema()

# Save the sampled dataset as a .csv file to the local project file system

smaller_all_flight_data_pd = smaller_all_flight_data.toPandas()
smaller_all_flight_data_pd.to_csv("data/preprocessed_flight_data.csv", index=False)
spark.stop()