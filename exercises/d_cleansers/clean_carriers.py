from pathlib import Path

import pyspark.sql.functions as psf
from pyspark.sql import SparkSession


def main(spark: SparkSession):
    repo_root = Path(__file__).parents[2]
    df = (
        spark.read.csv(
            str(repo_root / "data" / "raw_zone" / "carriers.csv"),
            header=True,
            sep=",",
        )
        .drop("_c2")
        .dropDuplicates(["AIRPORT"])
    )
    for colname in ("START_DATE_SOURCE", "THRU_DATE_SOURCE"):
        df = df.withColumn(colname, psf.to_date(colname))
    df = df.drop("_c4").distinct()

    df.write.parquet(
        path=str(repo_root / "clean_zone" / "carriers"),
        mode="overwrite",
    )


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    # Here, we set this option explicitly, so that we don't create 200 tiny
    # files from the call to distinct() / dropDuplicates()
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    main(spark)
