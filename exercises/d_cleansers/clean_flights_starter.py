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
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError

DEBUG = False


def naive_clean(frame: DataFrame) -> DataFrame:
    # TODO: workshop participants to implement this
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
        "spark.jars.packages": "",
        "fs.s3a.aws.credentials.provider": ""
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
    path_to_exercises = Path(__file__).parents[1]
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

    # Transform
    cleaned_frame = naive_clean(frame)

    if DEBUG:
        cleaned_frame.explain()  # instructional to see how Spark optimized the operations we requested.
        cleaned_frame.cache().show()
        cleaned_frame.printSchema()

    # Load
    cleaned_frame.write.parquet(
        path=str(target_dir / "cleaned_flights"),
        mode="overwrite",
        # Extra exercise: how much bigger are the files when the compression codec is set to "uncompressed"? And 'gzip'?
        compression="snappy",
    )
