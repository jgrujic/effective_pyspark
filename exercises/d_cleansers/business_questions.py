from pyspark.sql import SparkSession
from pathlib import Path
import pyspark.sql.functions as sf
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

def AA_flights(df_flights, df_carriers):
    df_flights = (df_flights.select("flight_date", col("unique_carrier").alias("carrier"), "flight_number", "arr_delay")
                .filter(col("flight_date").between("2011-01-01", "2011-12-31"))
    )

    df_carriers = (df_carriers
                .filter(col("carrier_name").startswith('American Airlines'))
    )
    final = df_flights.join(df_carriers, on="carrier", how="inner").cache()
    final.show()
    final.agg(sf.max("flight_date"), sf.min("flight_date")).show()
    return final.count(), final.filter(col("arr_delay")<10).count()


def delays_per_weekday(df):
    df = (df.select("flight_date", "dep_delay")
                 .withColumn("day_of_week", sf.dayofweek(sf.col("flight_date")))
    )
     
    day_dict = {
        "1": "Sunday", 
        "2": "Monday", 
        "3": "Tuesday", 
        "4": "Wednesday", 
        "5": "Thursday", 
        "6": "Friday", 
        "7": "Saturday"
        }
    df = df.groupBy("day_of_week").count().withColumn("day_name", col("day_of_week").cast(StringType()))
    return df.na.replace(day_dict, "day_name")

def delays_reasons(df):
 
    column_list = [c for c in df.columns if c.endswith("_delay_in_minutes")]
    df = df.select(*column_list) 
    for c in column_list:
        df = df.withColumn(c, sf.when(col(c)>0, sf.lit(1))) 
    df.show()
    exprs = {x: "sum" for x in column_list}
    print(column_list)
    df = df.agg(exprs)
    for c in df.columns:
        df = df.withColumnRenamed(c, c[4:-18]) 
    print(df.columns)
    df = df.withColumn("max_value", sf.greatest(*df.columns))

    # This work in DataBricks, but not here
    # df.printSchema()
    # df_pivot = df.drop("max_value")
    # df_pivot.show()
    # df_unpivoted = df_pivot.unpivot([], df_pivot.columns, "name", "val")
    # df_unpivoted.show()
    # # end of pivoting

    # cond = "sf.when" + ".when".join(["(col('" + c + "') == col('max_value'), sf.lit('" + c + "'))" for c in df.columns])
    # df = df.withColumn("MAX", eval(cond))\
 

    
    df = df.withColumn("biggest_cause_of_delays", sf.lit(None))
    for c in df.drop("max_value").columns:
        df = df.withColumn("biggest_cause_of_delays", sf.when(col(c)==col("max_value"), sf.lit(c)).otherwise(col("biggest_cause_of_delays")))
    return df


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    import time
#    time.sleep(60)

    path_to_exercises = Path(__file__).resolve().parents[1]
    target_dir = path_to_exercises / "target"
    resources_dir =  path_to_exercises / "resources"
    # Create the folder where the results of this script's ETL-pipeline will
    # be stored.
    target_dir.mkdir(exist_ok=True)
    path=str(target_dir / "cleaned_flights")
    clean_flights = spark.read.parquet(str(target_dir / "cleaned_flights"))
    clean_carriers = spark.read.csv(str(resources_dir / "carriers.csv"),header=True, )
    
    clean_flights.printSchema()
    n_flights, n_arr_delay = AA_flights(clean_flights, clean_carriers)
    print(f"number of flights og American airlines in 2011: {n_flights}, delay on less than 10 min in arrival: {n_arr_delay}")


    print("delays per week day Sunday =1, Monday = 2, etc")
    delays_per_weekday(clean_flights).show()
    
    print("delay reasons")
    delays_reasons(clean_flights).show()

    #time.sleep(2*60)

