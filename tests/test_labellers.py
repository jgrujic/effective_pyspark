import datetime as dt
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
)

from exercises.c_labellers.dates_solution import (
    label_weekend,
    is_belgian_holiday,
    label_holidays as label_holidays,  # Allows easier switching for people looking into the alternative forms. Simply replace the first word with label_holidays2 or label_holidays3.
)
from tests.comparers import assert_frames_functionally_equivalent

# This is not the best approach to creating a Spark Session for a test suite,
# since you only want one for the entire one. However, to explain that, we're
# going again into the domain of how testing frameworks like nose, pytest and
# unittest work, and that is not in the scope of this workshop. For pytest,
# read up on fixtures, if you want to know more.
spark = SparkSession.builder.master("local[*]").getOrCreate()

def test_pure_python_function():
    # Tests that don't initialize a SparkSession finish almost instantly. Try
    # this test and compare it to any other test in this file to compare tests
    # that depend on a SparkSession.
    # For this reason, tests involving Spark are typically run less often than
    # tests without Spark, though a good Continuous Integration system will
    # still run _all_ tests, before merging to the main branch!
    day1 = dt.date(2020, 7, 21)
    national_holiday = dt.date(2020, 7, 21)
    day_after_national_holiday = dt.date(2020, 7, 22)

    assert is_belgian_holiday(day1)
    assert is_belgian_holiday(national_holiday)
    assert not is_belgian_holiday(day_after_national_holiday)


def test_label_weekend():
    # Make sure to explore the large variety of useful functions in Spark:
    # https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html

    expected = spark.createDataFrame(
        data=[
            (date(2018, 5, 12), "a", True),
            (date(2019, 5, 13), "b", False),
            (date(2019, 5, 14), "c", False),
            (date(2019, 5, 15), "d", False),
            (date(2019, 5, 16), "e", False),
            (date(2019, 5, 17), "f", False),
            (date(2019, 5, 18), "g", True),
            (date(2019, 5, 19), "h", True),
        ],
        schema=(
            StructType()
            .add("date", DateType())
            .add("foo", StringType())
            .add("is_weekend", BooleanType())
        ),
    )

    frame_in = expected.select("date", "foo")

    actual = label_weekend(frame_in)
    assert_frames_functionally_equivalent(actual, expected)


def test_label_holidays():
    fields = [
        StructField("date", DateType(), True),
        StructField("is_belgian_holiday", BooleanType(), True),
        StructField("foobar", StringType(), True),
    ]
    expected = spark.createDataFrame(
        [
            (date(2000, 1, 1), True, "foo"),  # New Year's
            (date(2018, 7, 21), True, "bar"),  # Belgian national holiday
            (date(2019, 12, 6), False, "fubar"),  # Saint-Nicholas
            (None, None, "fubar"),
        ],
        schema=StructType(fields),
    )

    result = label_holidays(expected.drop("is_belgian_holiday"))

    assert_frames_functionally_equivalent(result, expected, False)

    # Notes: this test highlights well that tests are a form of up-to-date documentation.
    # It also protects somewhat against future changes (what if someone changes the
    # label_holidays to reflect the holidays of France?
    # Additionally, with the test written out already, you do not have to go into
    # your main function and write `print` everywhere as you are developing it. In fact,
    # most likely, you would have written out at some point something similar to this test,
    # put it in __main__ and once you noticed it succeeded, you would 've removed it. That's
    # so unfortunate! The automated test would've been lost and you force someone else to
    # have to rewrite it.


