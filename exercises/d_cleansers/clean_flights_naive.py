"""In this exercise, you are tasked with "cleaning" a dataset that you will get
  from object storage on a cloud service provider, specifically from AWS S3.

Depending on your capabilities, you should do either the short form of this
exercise or the long form.

In the short form you'll learn:
- how to use the Spark SQL functions reference page efficiently (pro-tip:
  bookmark the one for the Scala implementation, as the PySpark one lists
  functions alphabetically, which is inconvenient if you don't really know what
  you're looking for yet).
- how you can combine multiple Spark functions
- how you can write functions that operate on the virtual Columns, rather than
  DataFrames.

In the long form, you'll get a better understanding of how to structure such a
cleaning tasks, which is something that's quite a bit more easy in a general
purpose programming language, like Python, over e.g. a long SQL statement.

# Short exercise

1. use Spark to load the data that is on a private S3 bucket. That means you'll
   need to configure the SparkSession to authenticate itself to AWS, as well as
   to inform it how to interact with the S3 "file system". The instructor will
   provide you with temporary credentials which will allow you to read the data
   in the bucket. The same credentials will not allow you to write back to S3,
   since we want to limit the amount of data uploaded by our workshop
   participants.
2. Although the dataset contains more than 50 columns, for this short exercise,
   we're only interested in 2 columns: FL_DATE, representing the date on which
   a flight took off, and CSR_DEP_TIME, which is a representation of the local
   time at which the airplane took off. It is reminiscent of military time
   notation, but is not quite the same, which thus forms an extra challenge.

   With these 2 columns:

   1. correct the data type of the FL_DATE. Additionally, improve its column
      name. There's no good reason why data in a clean layer of your data lake
      / data warehouse should not be self describing and most modern DWHs won't
      limit the length of a column name in a significant way, so use this to
      your benefit and make self-documenting datasets.
   2. join the CSR_DEP_TIME with the FL_DATE info to get an actual timestamp.
      Few cases benefit from separating time information from the associated
      date, especially when it's about events that happened. Since Spark does
      not have a native time type, and CSR_DEP_TIME is related to an event
      (unlike e.g. a weekly schedule) the combination of date and time into a
      complete timestamp serves as a better basis for analytics. Note that data
      scientists may wish to extract values like hour and minutes from the time
      notation, but this is not something that should be done in the clean
      layer. Instead, such tasks are better for the use cases, which are in the
      business layer.

# Long exercise

Do the same as the short exercise. Now also clean all other columns. What is
typically meant by this is

- drop redundant columns, columns that can be derived from others. Again, for
  some use cases you may want items like "day of the month", but these are
  things that can be derived per use case. If you do it in the clean layer
  already, you won't know when to stop adding "features" (columns) and you also
  risk that people will waste time validating that your derived features are
  sound.
- choose the best data type. E.g. don't store dates as strings. This will
  affect the size of the data and so can improve performance.
- replace proxies for unknown values with the appropriate NULL value. You won't
  believe how many times people store an unknown date as 9999-12-31. While
  databases that don't support the SQL NULL value may have existed, there's
  few reasons to use a dummy value instead of null when you don't know. This
  has the advantage that you can actually use meaningful statistics, e.g.
  what's the max lease data of all cars in my fleet grouped by department?

All the work you do in "cleaning" a dataset pays off dividends for all use cases
that use this particular dataset.

 For a much quicker development workflow, copy the data first locally, so that
 you won't need to download the dataset over and over. Additionally, you won't
 need to use all data, so you may as well limit yourself to a subset of rows and
 columns.
"""
# For explanations on the columns, check https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGK

from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as sf
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    IntegerType,
    ShortType,
    StringType,
)
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError

from exercises.d_cleansers import cleaning_utilities as cu
from exercises.d_cleansers.clean_flights_improved import (
    a_shorter_cleaning_function,
)

DEBUG = False


def naive_clean(frame: DataFrame) -> DataFrame:
    # This cleaning function is a GOOD implementation: it does what is expected.
    # Most beginners will start by writing something like this. However, it is a
    # naive approach: it's not directly clear what happens where (renames occur at
    # the same time of casting the data types) and there's more typing than in the
    # alternative `a_shorter_cleaning_function`, which readers are recommended to
    # check out.

    # First, get the majority of columns “fixed”, i.e. their data types improved.
    df2 = (
        frame.withColumn("YEAR", sf.col("YEAR").cast(ShortType()))
        .withColumn("MONTH", sf.col("MONTH").cast(ByteType()))
        .withColumn("DAY_OF_MONTH", sf.col("DAY_OF_MONTH").cast(ByteType()))
        .withColumn("DAY_OF_WEEK", sf.col("DAY_OF_WEEK").cast(ByteType()))
        .withColumn("FL_DATE", sf.col("FL_DATE").cast(DateType()))
        .withColumn(
            "UNIQUE_CARRIER", sf.col("UNIQUE_CARRIER").cast(StringType())
        )
        .withColumn("TAIL_NUM", sf.col("TAIL_NUM").cast(StringType()))
        .withColumn(
            "FL_NUM", sf.col("FL_NUM").cast(ShortType())
        )  # values are between 1-8880, according to DataFrame.describe(), so this is fine
        .withColumn(
            "ORIGIN_AIRPORT_ID", sf.col("ORIGIN_AIRPORT_ID").cast(ShortType())
        )  # values are between 10k-17K
        .withColumn("ORIGIN", sf.col("ORIGIN").cast(StringType()))
        .withColumn(
            "ORIGIN_STATE_ABR", sf.col("ORIGIN_STATE_ABR").cast(StringType())
        )
        .withColumn(
            "DEST_AIRPORT_ID", sf.col("DEST_AIRPORT_ID").cast(IntegerType())
        )
        .withColumn("DEST", sf.col("DEST").cast(StringType()))
        .withColumn(
            "DEST_STATE_ABR", sf.col("DEST_STATE_ABR").cast(StringType())
        )
        .withColumn("CRS_DEP_TIME", sf.col("CRS_DEP_TIME").cast(StringType()))
        .withColumn("DEP_TIME", sf.col("DEP_TIME").cast(StringType()))
        .withColumn("DEP_DELAY", sf.col("DEP_DELAY").cast(ShortType()))
        .withColumn("DEP_DELAY_NEW", sf.col("DEP_DELAY_NEW").cast(ShortType()))
        .withColumn("DEP_DEL15", sf.col("DEP_DEL15").cast(ShortType()))
        .withColumn(
            "DEP_DELAY_GROUP", sf.col("DEP_DELAY_GROUP").cast(ShortType())
        )
        .withColumn("TAXI_OUT", sf.col("TAXI_OUT").cast(ShortType()))
        .withColumn("WHEELS_OFF", sf.col("WHEELS_OFF").cast(StringType()))
        .withColumn("WHEELS_ON", sf.col("WHEELS_ON").cast(StringType()))
        .withColumn("TAXI_IN", sf.col("TAXI_IN").cast(ShortType()))
        .withColumn("CRS_ARR_TIME", sf.col("CRS_ARR_TIME").cast(StringType()))
        .withColumn("ARR_TIME", sf.col("ARR_TIME").cast(StringType()))
        .withColumn("ARR_DELAY", sf.col("ARR_DELAY").cast(ShortType()))
        .withColumn("ARR_DELAY_NEW", sf.col("ARR_DELAY_NEW").cast(ShortType()))
        .withColumn("ARR_DEL15", sf.col("ARR_DEL15").cast(ShortType()))
        .withColumn(
            "ARR_DELAY_GROUP", sf.col("ARR_DELAY_GROUP").cast(ShortType())
        )
        .withColumn(
            "IS_CANCELLED",  # that's a personal preference: whenever I see a
            # boolean column, I believe it answers the question is_... or has_...
            # example: a column "broke" could indicate several things (what was
            # broken?), but a column "is_broke" simply answers the question
            # whether someone is financially bankrupt.
            sf.col("CANCELLED").cast(IntegerType()).cast(BooleanType()),
        )
        .withColumn(
            "CANCELLATION_CODE",
            sf.col("CANCELLATION_CODE").cast(StringType()),
        )
        .withColumn(
            "IS_DIVERTED",  # same explanation as CANCELLED
            sf.col("DIVERTED").cast(IntegerType()).cast(BooleanType()),
        )
        .withColumn(
            "CRS_ELAPSED_TIME", sf.col("CRS_ELAPSED_TIME").cast(ShortType())
        )
        .withColumn(
            "ACTUAL_ELAPSED_TIME",
            sf.col("ACTUAL_ELAPSED_TIME").cast(ShortType()),
        )
        .withColumn("AIR_TIME", sf.col("AIR_TIME").cast(ShortType()))
        .withColumn("FLIGHTS", sf.col("FLIGHTS").cast(ByteType()))
        .withColumn(
            "DISTANCE_IN_MILES", sf.col("DISTANCE").cast(IntegerType())
        )
        .withColumn(
            "DISTANCE_GROUP", sf.col("DISTANCE_GROUP").cast(ByteType())
        )  # values between 1-11 (DISTANCE_IN_MILES by 250 mile groups)
        # all delays are given in minutes
    ).drop(
        "_c44",  # this column is just empty, and unlabeled.
        # there's a column with no values in it, nor a name in the header (simply
        # a comma at the end of the header line)
        "CANCELLED",
        "DIVERTED",
    )

    # The tons of `withColumn("x", col("x").cast(some_data_type))` calls above are tedious.
    # A small improvement in terms of maintainability is this:
    for c in (
        "CARRIER_DELAY",
        "WEATHER_DELAY",
        "NAS_DELAY",
        "SECURITY_DELAY",
        "LATE_AIRCRAFT_DELAY",
    ):
        df2 = df2.withColumn(c, sf.col(c).cast(ShortType()))
    # And you could expand much more on this solution! See
    # 'a_shorter_cleaning_function', which is much shorter in terms of text
    # but might have the same number of lines as earlier, because of how
    # things get indented. Don't let yourself be fooled by that though:
    # it's the ease with which things can be changed and looked up that you
    # can gauge the quality of code.
    # Also notice how the code above does two things at the same time: did
    # you notice that some columns didn't just get a different data type
    # assigned, but also got their name changed? This can be confusing for
    # whoever is looking for the place where the columns got renamed.

    # Now, the columns that couldn't be dealt with by a mere “cast” can get
    # special care.
    # 1. Having year, month, day of month, day of week as columns that can be
    # derived from "fl_date" is ridiculous in a cleaned dataset. Such
    # derivations could be useful in later stages, like machine learning
    # pipelines, but then it's the *application owner* who should control the
    # transformations and give him/her the feeling that the data quality is high,
    # because he/she derives these new columns from source data. As an example:
    # do you know for sure that the combination of year-month-day_of_month
    # maches fl_date in every row?
    df3 = df2.drop("YEAR", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK")

    flight_date_col = df3["FL_DATE"]
    for c in ("ARR_TIME", "DEP_TIME", "TAXI_OUT", "TAXI_IN"):
        df3 = df3.withColumn(
            c,
            cu.combine_local_date_with_local_hour_minute_indication(
                flight_date_col, df3[c]
            ),
        )

    for colname in (
        "WHEELS_OFF",
        "WHEELS_ON",
        # There's at least one value in CRS_ARR_TIME that's at 2400. With a bit
        # of insight, you'll see then that CRS_DEP_TIME should also be handled
        # in the same way, even though in the sample you downloaded, this
        # column's values are below 2400.
        "CRS_ARR_TIME",
        "CRS_DEP_TIME",
    ):
        df3 = df3.withColumn(
            colname,
            cu.combine_date_with_overflowed_minutes(
                flight_date_col, df3[colname]
            ),
        )
    # ARR_DELAY, ARR_DELAY_NEW and ARR_DEL15 are derived columns.
    # They're the result of the formulas:
    # ARR_DELAY = ARR_TIME - CRS_ARR_TIME
    # ARR_DELAY_NEW = max(0, ARR_DELAY)
    # ARR_DELAY_15 = ARR_DELAY_NEW >= 15
    # ARR_DELAY_GROUP is also derivable: every (15-minutes from < -15 to >180)

    return df3.drop(
        "DEP_DELAY",
        "DEP_DELAY_NEW",
        "DEP_DEL15",
        "DEP_DELAY_GROUP",
        "ACTUAL_ELAPSED_TIME",
        "AIR_TIME",
    )


def load_data_from_local(path: Path):
    spark = SparkSession.builder.getOrCreate()
    return load_dataset_with_spark(spark=spark, location=str(path))


def load_non_local_data(location: str) -> DataFrame:
    config = {
        # note that this way of loading packages is okay for development, but in order to speed up your
        # Spark load time, you should store the required jars on the CLASSPATH that Spark searches.
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.0",
        "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
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
    repo_root = Path(__file__).parents[2]
    target_dir = repo_root / "data" / "clean_zone"
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
        location_of_local_data = repo_root / "data" / "raw_zone" / "flights"
        frame = load_data_from_local(location_of_local_data)
    except AnalysisException:
        print("Both attempting to load directly from cloud storage and from the"
              " local file system failed. Either implement the required "
              "configuration to load from the cloud storage or ensure you have "
              "the files locally (e.g. with the fallback.sh script).")
        import sys
        sys.exit(1)

    # Transform
    cleaned_frame = naive_clean(frame)
    # Alternatively, look at the following implementation and discuss the pros and cons to this approach.
    cleaned_frame = a_shorter_cleaning_function(frame)

    # Load
    if DEBUG:
        cleaned_frame.explain()  # instructional to see how Spark optimized the operations we requested.
        cleaned_frame.cache().show()
        cleaned_frame.printSchema()

    cleaned_frame.write.parquet(
        path=str(target_dir / "flights"),
        mode="overwrite",
        # Extra exercise: how much bigger are the files when the compression codec is set to "uncompressed"? And 'gzip'?
        compression="snappy",
    )
    # SPOILERS BELOW
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #
    #

    # assuming 8 files get made:
    # source files (for comparison): 333MB
    # uncompressed: 138MB
    # snappy: 91MB
    # gzip: 74MB
    # note that ordering of the rows also plays a role (run-length encoding at play here)
