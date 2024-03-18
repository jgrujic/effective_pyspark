from pyspark.sql import DataFrame, functions as sf
from pyspark.sql.types import ShortType, DateType, StringType

from exercises.d_cleansers import cleaning_utilities as cu


def a_shorter_cleaning_function(frame: DataFrame) -> DataFrame:
    # Note that this is not "shorter" because everything has pushed into
    # 4 functions. If you combine those three functions here, you still
    # get less code than the naive_clean(). However, splitting the
    # functionality into smaller blocks provides clarity: just from reading the
    # lines below, you get immediately an idea of what is happening, without
    # needing to read lots of details. This is known as choosing the right levels
    # of abstraction.
    for transformation in (
            drop_redundant_columns,
            correct_datatypes,
            improve_columns_names,
            make_timestamps_out_of_time_notations,
    ):
        frame = transformation(frame)
    return frame

    # An alternative, which uses method chaining, which some programmers prefer,
    # though it does require redundant typing of "transform", similar to chaining
    # withColumn a dozen times:
    # return (
    #     frame.transform(drop_redundant_columns)
    #     .transform(correct_datatypes)
    #     .transform(batch_rename_columns)
    # )
    # This is a *preference*: Spark will return the same query plan, so there's no
    # performance win.


def drop_redundant_columns(frame: DataFrame) -> DataFrame:
    easy_drops = {
        "YEAR",
        "MONTH",
        "DAY_OF_MONTH",
        "DAY_OF_WEEK",
        "DEP_DELAY_NEW",
        "DEP_DEL15",
        "DEP_DELAY_GROUP",
        "ARR_DELAY_NEW",
        "ARR_DEL15",
        "ARR_DELAY_GROUP",
        "DISTANCE_GROUP",
        "FLIGHTS",  # always 1.0, probably a remnant from some grouping logic
        # _c44 is present because of the empty column at the end: Spark
        # automatically assigns column names. This is the 45th column, Spark
        # also starts counting from 0.
        "_c44",
    }
    return frame.drop(*easy_drops)


def correct_datatypes(frame: DataFrame) -> DataFrame:
    mapping = {
        ShortType: {
            "FL_NUM",  # 1-8880
            "CRS_ARR_TIME",
            "CRS_DEP_TIME",
            "ARR_TIME",
            "DEP_TIME",
            "WHEELS_OFF",  # 1-2400, always leftpadded to 4 digits
            "WHEELS_ON",  # 1-2400, always letpadded to 4 digits
            "ORIGIN_AIRPORT_ID",  # numbers between 10135 and 16218
            "DEST_AIRPORT_ID",  # same comment as ORIGIN_AIRPORT_ID
            "TAXI_OUT",  # values between 1.00 and 173.00, all with zeroes behind the period
            "TAXI_IN",  # Same, range between 1.00 and 161.00
            "DISTANCE",  # Earth's circumference in miles is 24901, which still fits in a Short.
            # The various x_DELAY columns are expressed in minutes. With the
            # maximum value for a short (2^15-1), one could easily get to 22
            # days that way. It is unlikely any airline would resume a flight
            # after 22 days: they'd create a new one.
            "CARRIER_DELAY",
            "WEATHER_DELAY",
            "NAS_DELAY",
            "SECURITY_DELAY",
            "LATE_AIRCRAFT_DELAY",
            "DEP_DELAY",
            "ARR_DELAY",
            # The following x_TIME columns follow a similar reasoning as the _DELAY columns
            "CRS_ELAPSED_TIME",
            "ACTUAL_ELAPSED_TIME",
            "AIR_TIME",
        },
        DateType: {
            "FL_DATE",
        },
        StringType: {
            "UNIQUE_CARRIER",  # always 2 characters
            "TAIL_NUM",  # nullable, 5 or 6 chars
            "DEST",
            "ORIGIN",  # 3 chars
            "DEST_STATE_ABR",
            "ORIGIN_STATE_ABR",  # 2 chars, in principle derivable from ORIGIN
            "CANCELLATION_CODE",  # 1 char
            "CANCELLED",  # to be converted to bool with a special function
            "DIVERTED",  # to be converted to bool with a special function
        },
    }

    for datatype, colnames in mapping.items():
        for colname in colnames:
            frame = frame.withColumn(
                colname, sf.col(colname).cast(datatype())
            )

    for column_name in ("CANCELLED", "DIVERTED"):
        # Spark can "automatically" convert values like 1 and 0, even "y" and "n" to bool, but not 1.0 and 0.0
        frame = frame.withColumn(
            column_name,
            cu.to_bool(column_name, true_values={"1.0"}, false_values={"0.0"}),
        )
    return frame


def make_timestamps_out_of_time_notations(frame: DataFrame) -> DataFrame:
    # Remark: all times are given in the local time of the airport. That would
    # be okay, if we'd also have the flight date in local time, which is not
    # the case. We don't have the date of arrival in local time and there is no
    # straightforward way to derive it, as we'd need the local time zone info:
    # you could add the flight duration to the departure time, but then you'd
    # get the arrival time in the departure timezone, which you could convert
    # to the arrival timezone, if you had both names!
    # For this exercise, we ignore time zones, as we'd need to have a table
    # giving the Olson timezone information (e.g. Europe/Brussels) per airport.
    # We also assume (erroneously!) that the local arrival date is the same as
    # the local departure date.

    for col in (
        "scheduled_departure_time",
        "dep_time",
        "wheels_off",
        "scheduled_arrival_time",
        "arr_time",
        "wheels_on",
    ):
        frame = cu.combine_date_with_time_with_graceful_time_overflow(
            frame, "flight_date", col
        )
    return frame


def improve_columns_names(frame: DataFrame) -> DataFrame:
    renames = {
        "FL_DATE": "flight_date",
        # Something that contains letters as well digits should not be called
        # a "number", that's just plain confusing:
        "TAIL_NUM": "tail_code",
        "FL_NUM": "flight_number",
        # You could take two airports and cross-reference the distance between
        # them. In principle, "distance" here isn't clear enough: is it the
        # distance between the airports, or is it the distance flown
        # (an airplane does not need to follow a straight line)? In this case,
        # it's the former. And then one could actually derive that. However,
        # such a lookup (or even the great-circle-distance calculation) isn't
        # exactly cheap either. A lookup requires a join and the great-circle-
        # distance a lot of math involving trigonometric functions which
        # require more CPU cycles than a simple sum or a square.
        "DISTANCE": "distance_between_airports_in_miles",
        "CRS_ARR_TIME": "scheduled_arrival_time",
        "CRS_DEP_TIME": "scheduled_departure_time",
        # boolean columns are better served with an 'is_' or 'has_' prefix.
        "CANCELLED": "is_cancelled",
        "DIVERTED": "is_diverted",
        "CRS_ELAPSED_TIME": "planned_duration_in_minutes",
        "ACTUAL_ELAPSED_TIME": "actual_duration_in_minutes",
        "AIR_TIME": "in_air_duration_in_minutes",
    }
    duration_renames = {
        k.upper(): f"{k}_in_minutes"
        for k in (
            "carrier_delay",
            "weather_delay",
            "nas_delay",
            "security_delay",
            "late_aircraft_delay",
        )
    }
    renames.update(duration_renames)
    frame = cu.batch_rename_columns(frame, renames)
    # let's also lowercase all column names, to offer a similar style (and less pressing of SHIFT with the pinky)
    return cu.lowercase_column_names(frame)
