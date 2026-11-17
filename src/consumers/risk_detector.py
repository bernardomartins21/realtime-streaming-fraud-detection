from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col, collect_list

spark = SparkSession.builder.appName("FinancialRiskDetector").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()

transactional_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "transactions-raw") \
    .option("startingOffsets", "earliest").load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), transactional_schema).alias("data")) \
    .select("data.*")

df_risk_alerts = df_parsed.filter(col("amount") > 800)

query = df_risk_alerts.writeStream.format("console").outputMode("append").option("checkpointLocation", "./tmp/checkpoints/risk_detector").start()

query.awaitTermination()