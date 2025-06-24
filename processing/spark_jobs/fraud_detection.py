from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()
df = spark.read.json("s3://your-bucket/transactions/")

# Basic fraud logic: high amount or multiple transactions in short time
fraud_df = df.withColumn("is_fraud", when(col("amount") > 10000, 1).otherwise(0))

fraud_df.write.parquet("s3://your-bucket/output/fraud_detected/", mode="overwrite")
