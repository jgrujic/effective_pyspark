import datetime

import holidays
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, dayofweek, udf, min as sfmin, max as sfmax
from pyspark.sql.types import BooleanType, DateType, StructField, StructType

MIN_YEAR_FOR_HOLIDAYS = 2000
MAX_YEAR_FOR_HOLIDAYS = 2020


def is_belgian_holiday(date: datetime.date) -> bool:
    belgian_holidays = holidays.BE()
    return date in belgian_holidays


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    # The line below is one solution. There is a more performant version of it
    # too, involving the modulo operator, but it is more complex. The
    # performance gain might not outweigh the cost of a programmer trying to
    # understand that arithmetic.
    # Note that 1 and 7 below are technically magic numbers again. The developer
    # experience would be better if someone would define these as constants,
    # "SATURDAY = 7" e.g.

    # Finally, programming languages and even frameworks within the same
    # programming language tend to differ in the convention whether Monday is
    # day 1 or not. You should always check the documentation corresponding to
    # your library. In Europe, most people consider Monday the first day of
    # the week. For Spark, which depends on Java, it is different.
    return frame.withColumn(new_colname, dayofweek(colname).isin(1, 7))


def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Add a column indicating whether the column `colname`
    is a holiday."""

    # holiday_udf = udf(lambda z: is_belgian_holiday(z), BooleanType())

    # If you were to see something like the line above in serious code, the
    # author of that line might not have understood the concepts of lambda
    # functions (nameless functions) and function references well. The
    # assignment above is more efficiently written as:
    # holiday_udf = udf(is_belgian_holiday, BooleanType())
    # That being said, if you need to account for unknown values (which in
    # the SQL world are represented by NULL and in Python by None), then you
    # should "augment" the original is_belgian_holiday function, which cannot
    # handle a check like `None in holidays.BE()` as it expects a date for the
    # __contains__ method (which is what `a in b` calls). You can do that e.g.
    # with a lambda, like this:
    holiday_udf = udf(lambda x: x if x is None else is_belgian_holiday(x), BooleanType())
    return frame.withColumn(new_colname, holiday_udf(col(colname)))


def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Add a column indicating whether the column `colname`
    is a holiday."""

    # A more efficient implementation of `label_holidays` than the udf-variant.
    # Major downside is that the range of years needs to be known a priori. Put
    # them in a config file or extract the range from the data beforehand.
    # If you really have no clue what the range of years is (in most realistic
    # situations you do), then you have no choice but to traverse the data at
    # least one extra time, which you can do with an aggregation like this:
    # min_year, max_year = [x.year for x in frame.agg(sfmin(colname), sfmax(colname)).head()]
    holidays_be = holidays.BE(
        years=list(range(MIN_YEAR_FOR_HOLIDAYS, MAX_YEAR_FOR_HOLIDAYS))
    )
    return frame.withColumn(
        new_colname, col(colname).isin(list(holidays_be.keys()))
    )


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Add a column indicating whether the column `colname`
    is a holiday."""

    # Another more efficient implementation of `label_holidays`. Same downsides
    # as label_holidays2, but scales better.
    holidays_be = holidays.BE(
        years=list(range(MIN_YEAR_FOR_HOLIDAYS, MAX_YEAR_FOR_HOLIDAYS))
    )

    # Since you're expecting a pyspark.sql.DataFrame in this function, you
    # *know* there's an existing SparkSession. You can get it like this:
    spark = SparkSession.getActiveSession()
    # alternatively, like this:
    # spark = frame.sql_ctx.sparkSession
    # (in newer Spark versions: spark = frame.sparkSession)
    # Both return the same session. The latter is guaranteed to return a
    # SparkSession, the former only does it best effort and won't complain if
    # there's no active SparkSession. Static type checking tools, like mypy,
    # will raise warnings on that.

    holidays_frame = spark.createDataFrame(
        data=[(day, True) for day in holidays_be.keys()],
        schema=StructType(
            [
                StructField(colname, DateType(), False),
                StructField(new_colname, BooleanType(), False),
            ]
        ),
    )
    # We'll add some intermediate visuals here. In production, you wouldn't do
    # this. This is for learning purposes only, so you can understand what the
    # intermediate frames look like.
    holidays_frame.show(51)
    holidays_frame.printSchema()
    part1 = frame.join(holidays_frame, on=colname, how="left")

    part2 = part1.na.fill(False, subset=[new_colname])
    part1.show()
    part2.show()
    part2.printSchema()
    return part2
