from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, avg, stddev, row_number
from pyspark.sql.window import Window
import requests

# Kafka and Flask configurations
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'cryptocurrency'
FLASK_ENDPOINT = 'http://127.0.0.1:5000/stream'  # Flask endpoint for receiving data

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToFlaskStreaming") \
    .getOrCreate()

# Define schema for the Kafka messages
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", StringType(), True),
    StructField("open", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("timestamp", DoubleType(), True)
])

# Read data from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Process Kafka data
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp to a readable format
df = df.withColumn("timestamp_readable", from_unixtime(col("timestamp")).alias("timestamp_readable"))

# Convert "price" and "volume" columns to numeric types for calculations
df = df.withColumn("close", col("close").cast("double"))
df = df.withColumn("volume", col("volume").cast("double"))

# Calculate RSI - Relative Strength Index
def calculate_rsi(df, window_size=14):
    """Calculate RSI (Relative Strength Index) for Spark DataFrame."""
    # Calculate price change
    df = df.withColumn("price_change", col("close") - lag("close", 1).over(Window.orderBy("timestamp")))

    # Calculate gain and loss
    df = df.withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0))
    df = df.withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0))

    # Calculate average gain and loss over the window
    windowSpec = Window.orderBy("timestamp").rowsBetween(-window_size, 0)
    df = df.withColumn("avg_gain", avg("gain").over(windowSpec))
    df = df.withColumn("avg_loss", avg("loss").over(windowSpec))

    # Calculate RSI
    df = df.withColumn("rs", col("avg_gain") / col("avg_loss"))
    df = df.withColumn("rsi", 100 - (100 / (1 + col("rs"))))

    return df

# Calculate Bollinger Bands
def calculate_bollinger_bands(df, window_size=20):
    """Calculate Bollinger Bands for Spark DataFrame."""
    # Calculate moving average and standard deviation
    windowSpec = Window.orderBy("timestamp").rowsBetween(-window_size, 0)
    df = df.withColumn("moving_avg", avg("close").over(windowSpec))
    df = df.withColumn("stddev", stddev("close").over(windowSpec))

    # Calculate Bollinger Bands
    df = df.withColumn("bb_high", col("moving_avg") + 2 * col("stddev"))
    df = df.withColumn("bb_low", col("moving_avg") - 2 * col("stddev"))
    df = df.withColumn("bb_mid", col("moving_avg"))

    return df

# Calculate MACD (Moving Average Convergence Divergence)
def calculate_macd(df, short_window=12, long_window=26, signal_window=9):
    """Calculate MACD for Spark DataFrame."""
    # Calculate short-term and long-term EMAs
    df = ema(df, "close", short_window, "ema_short")
    df = ema(df, "close", long_window, "ema_long")

    # Calculate MACD
    df = df.withColumn("macd", col("ema_short") - col("ema_long"))

    # Calculate MACD signal
    df = df.withColumn("macd_signal", avg("macd").over(Window.orderBy("timestamp").rowsBetween(-signal_window, 0)))

    # Calculate MACD histogram
    df = df.withColumn("macd_hist", col("macd") - col("macd_signal"))

    return df

def ema(df, column, window_size, alias):
    """Calculate Exponential Moving Average (EMA) for Spark DataFrame."""
    alpha = 2 / (window_size + 1)
    df = df.withColumn(alias, when(
        row_number().over(Window.orderBy("timestamp")) > window_size,
        col(column) * alpha + lag(alias, 1).over(Window.orderBy("timestamp")) * (1 - alpha)
    ).otherwise(col(column)))
    return df

# Send processed data to Flask
def send_to_flask(partition):
    for row in partition:
        data = {
            "symbol": row["symbol"],
            "price": row["close"],
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "timestamp": row["timestamp_readable"],
            "rsi": row["rsi"],
            "bb_high": row["bb_high"],
            "bb_low": row["bb_low"],
            "bb_mid": row["bb_mid"],
            "macd": row["macd"],
            "macd_signal": row["macd_signal"],
            "macd_hist": row["macd_hist"]
        }
        try:
            response = requests.post(FLASK_ENDPOINT, json=data)
            if response.status_code != 200:
                print(f"Failed to send data: {response.status_code}")
        except Exception as e:
            print(f"Error sending data: {e}")

# Apply technical indicators
df = calculate_rsi(df)
df = calculate_bollinger_bands(df)
df = calculate_macd(df)

# Write data to Flask 
df.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.foreachPartition(send_to_flask)) \
    .outputMode("append") \
    .start() \
    .awaitTermination()

