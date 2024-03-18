# For explanations on the columns, check https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGK

from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as sf
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    IntegerType,
    ShortType,
    StringType,
)

DEBUG = False



def naive_clean(frame: DataFrame) -> DataFrame:
       
    frame = drop_extra_columns(frame)   
    # frame = correct_timestamps(frame) 
    # frame = rename_and_cast_columns(frame)  
    print(frame.columns)
    frame.show()
    return frame



def drop_extra_columns(df):
    # dropping redundant columns which can be derived from other columns
    drop_list = [ 
       "YEAR",
        "MONTH",
        "DAY_OF_MONTH",
        "DAY_OF_WEEK", 
        "DEP_DELAY",
        "DEP_DELAY_NEW",
        "DEP_DEL15",
        "DEP_DELAY_GROUP",
        "ARR_DELAY",
        "ARR_DELAY_NEW",
        "ARR_DEL15",
        "ARR_DELAY_GROUP",
        "_c44",
    ]
    return df.drop(*drop_list)

def rename_and_cast_columns(df):
    # dropping redundant columns which can be derived from other columns
    list_str = [
        'UNIQUE_CARRIER',  'ORIGIN_AIRPORT_ID', 'ORIGIN', 'ORIGIN_STATE_ABR', 'DEST_AIRPORT_ID', 'DEST', 'DEST_STATE_ABR', 'CANCELLATION_CODE',  'CRS_ELAPSED_TIME', 
        'FLIGHTS', 'DISTANCE', 'DISTANCE_GROUP'
    ]
    
    for c in list_str:
        df = df.withColumn(c.lower(), col(c))

    list_int = [
        'TAIL_NUM', 'FL_NUM',
        'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY', 
        'AIR_TIME', 'ACTUAL_ELAPSED_TIME', 'TAXI_OUT', 'TAXI_IN',
    ]

    for c in list_int:
        df = df.withColumn(c.lower(), col(c).cast(ShortType()))


    list_bool =[
        'CANCELLED', 'DIVERTED',
    ]
    for c in list_bool:
        df = df.withColumn(c.lower(), col(c).cast(ShortType()).cast(BooleanType()))
    df.select("cancelled", "diverted").show()    
    return df



def correct_timestamps(frame: DataFrame) -> DataFrame:
    # TODO: workshop participants to implement this
    # time is string of time 900, menaing 9h, add it to date and make timestamp
    time_list=[
        "CRS_DEP_TIME",
        "DEP_TIME",
        "WHEELS_OFF",
        "CRS_ARR_TIME",
        "ARR_TIME",
        "WHEELS_ON",
    ]
    frame = frame.withColumn("flight_date", sf.to_timestamp("FL_DATE")).drop("FL_DATE") 
    for c in time_list:
        frame = (frame
            .withColumn("tmp", col(c).cast(IntegerType()))
            .withColumn("hour", sf.floor(col("tmp")/100))
            .withColumn("min", col("tmp") % 100 )
            .withColumn(c.lower(), 
               sf.from_unixtime(sf.unix_timestamp("flight_date") + col("hour")*3600 + col("min")*60))
            .drop("tmp", "hour", "min")
    )
    return frame

def load_data_from_local(path: Path):
    spark = SparkSession.builder.getOrCreate()
    return load_dataset_with_spark(spark=spark, location=str(path))


def load_non_local_data(location: str) -> DataFrame:
    # TODO: workshop participants to complete this
    # TIP: Revise the slides the instructor has shown again.
    # TIP: if you really can't find it out, you can execute the fallback.sh
    # script in this folder to get a local copy of the CSV dataset.
    config = {
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.0",
        "fs.s3a.aws.credentials.provider":"com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    conf = SparkConf().setAll(config.items())
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return load_dataset_with_spark(spark=spark, location=location)


def load_dataset_with_spark(spark: SparkSession, location: str) -> DataFrame:
    return spark.read.csv(
        location,
        # For a CSV, `inferSchema=False` means every column stays of the string
        # type. There is no time wasted on inferring the schema, which is
        # arguably not something you would depend on in production either.
        inferSchema=False,
        header=True,
        # The dataset mixes two values for null: sometimes there's an empty attribute,
        # which you will see in the CSV file as two neighboring commas. But there are
        # also literal "null" strings, like in this sample: `420.0,null,,1.0,`
        # The following option makes a literal null-string equivalent to the empty value.
        nullValue="null",
    )


if __name__ == "__main__":
    # Let's define some variables that you should ideally be getting from
    # configuration, instead of hardcoding them like we do here.

    location_of_data_in_the_cloud = "s3a://dmacademy-course-assets/pyspark/airline_subset"
    
    # Here's a good practice: use relative paths, so that the location of this
    # project on your system won't mean editing paths.
    path_to_exercises = Path(__file__).resolve().parents[1]
    target_dir = path_to_exercises / "target"
    # Create the folder where the results of this script's ETL-pipeline will
    # be stored.
    target_dir.mkdir(exist_ok=True)

    # Extract
    try:
        frame = load_non_local_data(location_of_data_in_the_cloud)
    except Py4JJavaError as e:
        # We've added this block in case you have the dataset locally already and want to
        # develop faster. However, for the end goal, you should be getting your data from
        # the cloud service provider.
        resources_dir = path_to_exercises / "resources"
        location_of_local_data = resources_dir / "flights"
        frame = load_data_from_local(location_of_local_data)
    except AnalysisException:
        print("Both attempting to load directly from cloud storage and from the"
              " local file system failed. Either implement the required "
              "configuration to load from the cloud storage or ensure you have "
              "the files locally (e.g. with the fallback.sh script).")
        import sys
        sys.exit(1)
 
     
    # frame = frame.limit(1000) 
    # cleaned_frame = frame
    # Transform
    cleaned_frame = naive_clean(frame)
    # cleaned_frame = a_shorter_cleaning_function(frame)

    #cleaned_frame.show()
    if DEBUG:
        cleaned_frame.explain()  # instructional to see how Spark optimized the operations we requested.
        cleaned_frame.cache().show()
        cleaned_frame.printSchema()

    # Load
    cleaned_frame.write.parquet(
        path=str(target_dir / "cleaned_flights_snappy"),
        mode="overwrite",
        # Extra exercise: how much bigger are the files when the compression codec is set to "uncompressed"? And 'gzip'?
        compression="snappy",
    )
       # Load
    cleaned_frame.write.parquet(
        path=str(target_dir / "cleaned_flights_uncompressed"),
        mode="overwrite",
        # Extra exercise: how much bigger are the files when the compression codec is set to "uncompressed"? And 'gzip'?
        compression="uncompressed",
    )
    cleaned_frame.write.parquet(
        path=str(target_dir / "cleaned_flights_gzip"),
        mode="overwrite",
        # Extra exercise: how much bigger are the files when the compression codec is set to "uncompressed"? And 'gzip'?
        compression="gzip",
    )