try:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.getOrCreate()
    print("Using Databricks Connect (remote Spark)")

except Exception as e_db:  # <-- catch all, not just ImportError
    print("Databricks Connect not available:", type(e_db).__name__, "-", e_db)

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()
        print("Using local SparkSession")

    except Exception as e_local:  # <-- catch all here too
        raise RuntimeError(
            "Neither Databricks Connect nor local PySpark can start.\n"
            f"Databricks error: {type(e_db).__name__}: {e_db}\n"
            f"Local Spark error: {type(e_local).__name__}: {e_local}"
        )
