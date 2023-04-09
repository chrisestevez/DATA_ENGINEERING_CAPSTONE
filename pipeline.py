#!/usr/bin/env python3
from pyspark.sql import SparkSession
from project_lib import ETL
from project_lib import Utilities
spark = SparkSession.builder.appName("incident_data_pipeline").getOrCreate()
util = Utilities()
util.initialize_project()

pipeline = ETL(spark)
pipeline.main()

spark.stop()