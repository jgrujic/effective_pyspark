"""Your task is to complete each of these skeleton functions: only the function
definition is given, but you need to fill in the body. You can check that
you're doing things right, by running the associated tests in
tests/test_labellers.py. You should not modify the tests! For the last exercise
here, the one about labelling, with Spark, Belgian holidays, you may alter the
import statement at the top of the test module to reference the right alternative."""
import datetime

from pyspark.sql import DataFrame


def is_belgian_holiday(date: datetime.date) -> bool:
    pass


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    """Adds a column indicating whether the attribute `colname`
    in the corresponding row is a weekend day."""
    pass


def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether the column `colname`
    is a holiday."""
    pass


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
    pass


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether the column `colname`
    is a holiday. An alternative implementation."""
    pass
