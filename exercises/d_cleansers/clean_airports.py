from pathlib import Path

from pyspark.sql import SparkSession


def main(spark: SparkSession):
    repo_root = Path(__file__).parents[2]
    df = (
        spark.read.csv(
            str(repo_root / "data" / "raw_zone" / "airports"),
            header=True,
            sep=",",
        )
        .drop("_c2")
        .dropDuplicates(["AIRPORT"])
    )

    # There is no meaningful cleaning to be done, simply change the
    # serialization format to Parquet and be done with it.
    df.write.parquet(
        path=str(repo_root / "data" / "clean_zone" / "airports"),
        mode="overwrite",
    )


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    # These dimension tables are so small, that Spark isn't the best tool to
    # process them.  However, it does offer a lot of convenience functions,
    # especially when working in a cloud environment. Consider pandas for local
    # development on such tiny datasets or even pure Python.

    # Here, we set this option explicitly, so that we don't create 200 tiny
    # files from the call to distinct() / dropDuplicates()
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    main(spark)
