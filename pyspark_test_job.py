"""Simple PySpark test job for Spark 3.5.1.

Run with:
    spark-submit test-job.py
    spark-submit --master "local[2]" test-job.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder.appName("cma-test-job").getOrCreate()
    try:
        print(f"Spark version: {spark.version}")

        data = [
            (1, "a", 10),
            (2, "a", 20),
            (3, "b", 30),
            (4, "b", 40),
            (5, "c", 50),
        ]
        df = spark.createDataFrame(data, ["id", "category", "value"])

        result = (
            df.groupBy("category")
            .agg(
                F.count("*").alias("cnt"),
                F.sum("value").alias("total"),
                F.avg("value").alias("average"),
            )
            .orderBy("category")
        )
        result.show()

        row_count = df.count()
        total = df.agg(F.sum("value")).collect()[0][0]
        print(f"Row count: {row_count}, total value: {total}")

        assert row_count == 5, f"Expected 5 rows, got {row_count}"
        assert total == 150, f"Expected total 150, got {total}"
        print("Test job completed successfully")
    except Exception as e:
        print(f"Test job failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
