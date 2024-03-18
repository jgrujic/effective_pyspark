"""Your task is to complete each of these skeleton functions: only the function
definition is given, but you need to fill in the body. You can check that
you're doing things right, by running the associated tests in
tests/test_labellers.py. You should not modify the tests! For the last exercise
here, the one about labelling, with Spark, Belgian holidays, you may alter the
import statement at the top of the test module to reference the right alternative."""
import datetime

from pyspark.sql import DataFrame
def is_belgian_holiday(date: datetime.date) -> bool:
    return date in holiday.BE()


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    """Adds a column indicating whether the attribute `colname`
    in the corresponding row is a weekend day."""
  
    return (frame
            .withColumn("day_of_week", dayofweek(sf.col("colname")))
            .withColumn(new_colname, sf.when(sf.col("day_of_week").isin([1, 7], True).otherwise(False)))    
    )

def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether the column `colname`
    is a holiday."""
    udf_is_belgian_holiday = sf.udf(is_begian_holiday, ReturnType=BooleanType())
    return (frame
            .withColumn(new_colname, udf_is_belgina_holiday(col(colname)))
    )


# If you find a working solutions for label_holidays, try to find one or
# two alternatives that uses a completely different approach. Then discuss the
# pros and cons to each alternative.
def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether the column `colname`
    is a holiday. An alternative implementation."""

    min_year = frame.agg({colname: "min"}).collect()[0].year
    max_year = frame.agg({colname: "max"}).collect()[0].year
    holidays_be = holiday.BE(years=range(min_year, max_year))
    return frame.withColumn(new_colname, col(colname).isin(holdays_be))


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether the column `colname`
    is a holiday. An alternative implementation."""
    pass
