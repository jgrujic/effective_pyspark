from typing import Collection, Mapping, Union

from pyspark.sql import Column, functions as sf, DataFrame

ColumnOrStr = Union[Column, str]


def to_bool(
    c: str, true_values: Collection[str], false_values: Collection[str]
) -> Column:
    """If the values in the column named c match any of the values in the
    collection of true_values, the value will become True. Otherwise, if it
    matches any of those in the Collection of false_Values, it will convert the
    value to False. If neither matches, return null."""
    return sf.when(
        sf.col(c).isin(true_values), True
    ).otherwise(
        sf.when(sf.col(c).isin(false_values), False)
    )


def combine_date_with_time_with_graceful_time_overflow(
    frame: DataFrame, date_col: str, time_col: str
) -> DataFrame:
    # some of the time_col columns go from 0 to 2400 (inclusive), which Spark
    # will rightfully claim is not a correct time format. Those edge cases are
    # solved with a when-clause. The non-edge cases are dealt with by simply
    # concatenating the pieces and parsing it using the correct timestamp
    # format.
    return frame.withColumn(
        time_col,
        sf.when(frame[time_col] == 2400, sf.date_add(date_col, 1)).otherwise(
            combine_local_date_with_local_hour_minute_indication(
                date_col, time_col
            )
        ),
    )


def lowercase_column_names(frame: DataFrame) -> DataFrame:
    mapping = {c: c.lower() for c in frame.columns}
    return batch_rename_columns(frame, mapping)


def batch_rename_columns(
    df: DataFrame, mapping: Mapping[str, str]
) -> DataFrame:
    # Note that this function has been implemented in a later version of Spark
    # than the one we're using. This only comes to show that short utility
    # functions serve a good purpose and you should be crafty enough to write
    # these when the functionality doesn't come out of the box just yet.
    for old_name, new_name in mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def combine_date_with_overflowed_minutes(
    datecolumn: ColumnOrStr, hour_minute_column: ColumnOrStr
) -> Column:
    # An alternative to the earlier function, but this one will handle
    # hour_minute values of 2400, however it will have issues with daylight
    # savings time.

    # Also note that this function assumes the datecolumn has an associated
    # data type of date. If you pass it a string, it won't work, because
    # unix_timestamp has a default choice for its format specification.

    # The key aspect to note is that "when" an event occurred is a specific
    # timestamp, given by a combination of a date, time and timezone. The
    # timezone is often implicit (which isn't good form). You should always be
    # explicit and provide all these fields. Do not disconnect time from the date,
    # because saying "my train left too early at 11:03" will not help the people
    # at the railway services, without also providing the date (and train number)
    daystart_as_seconds_since_epoch = sf.unix_timestamp(datecolumn)
    hour_minutes = sf.lpad(hour_minute_column, 4, "0")
    hours = sf.substring(hour_minutes, 1, 2)
    minutes = sf.substring(hour_minutes, 3, 2)
    total_seconds = hours * 3600 + minutes * 60
    return sf.from_unixtime(daystart_as_seconds_since_epoch + total_seconds)


def combine_local_date_with_local_hour_minute_indication(
    datecolumn: ColumnOrStr, hour_minute_column: ColumnOrStr
) -> Column:
    """Combines values of "2020-10-16", with 1405 into "2020-10-16 14:05:00."""

    hms_string = sf.lpad(hour_minute_column, 4, "0")
    return sf.to_timestamp(
        sf.concat_ws(" ", sf.date_format(datecolumn, "yyyy-MM-dd"), hms_string),
        format="yyyy-MM-dd HHmm",
    )
