from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, avg, stddev, row_number, from_unixtime
from pyspark.sql.window import Window
import requests
import numpy as np
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler

# Kafka and Flask configurations
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'cryptocurrency'
FLASK_ENDPOINT = 'http://127.0.0.1:5000/stream'  # Flask endpoint for receiving data

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToFlaskStreaming") \
    .getOrCreate()

# Load LSTM models for both BTC and ETH
model_btc = load_model('lstm_model_btc.h5')
model_eth = load_model('lstm_model_eth.h5')  

# Initialize scalers for both cryptocurrencies
scaler_btc = MinMaxScaler(feature_range=(0, 1))
scaler_eth = MinMaxScaler(feature_range=(0, 1))

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

# Function to make prediction using LSTM models
def make_prediction(prices, model, scaler, sequence_length=3):
    """
    Helper function to make predictions using the LSTM model
    """
    # Prepare input data (sequence of prices)
    input_data = np.array(prices[-sequence_length:]).reshape(-1, 1)

    # Normalize the input data
    input_data_scaled = scaler.fit_transform(input_data)
    input_data_scaled = np.reshape(input_data_scaled, (1, sequence_length, 1))  # Reshaping for LSTM

    # Make prediction
    prediction = model.predict(input_data_scaled)

    # Inverse transform the prediction
    predicted_price_scaled = float(prediction[0][0])
    predicted_price = scaler.inverse_transform([[predicted_price_scaled]])[0][0]

    return predicted_price

# Function to send processed data to Flask
def send_to_flask(partition):
    for row in partition:
        symbol = row["symbol"]
        open_price = row["open"]
        high_price = row["high"]
        low_price = row["low"]
        close_price = row["close"]
        price = row["price"]
        volume = row["volume"]
        timestamp = row["timestamp_readable"]
        
        # Prepare the price sequence for prediction
        price_sequence = [open_price, high_price, low_price, close_price, price]

        # Handle BTC predictions
        if symbol == 'BTCUSDT':
            predicted_price = make_prediction(price_sequence, model_btc, scaler_btc)
            response_data = {
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "timestamp": timestamp,
                "predicted_price": predicted_price
            }

        # Handle ETH predictions
        elif symbol == 'ETHUSDT':
            predicted_price = make_prediction(price_sequence, model_eth, scaler_eth)
            response_data = {
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "timestamp": timestamp,
                "predicted_price": predicted_price
            }

        else:
            print(f"Unsupported symbol: {symbol}")
            continue

        # Send the processed data to Flask
        try:
            response = requests.post(FLASK_ENDPOINT, json=response_data)
            if response.status_code != 200:
                print(f"Failed to send data: {response.status_code}")
        except Exception as e:
            print(f"Error sending data to Flask: {e}")

# Write data to Flask 
df.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.foreachPartition(send_to_flask)) \
    .outputMode("append") \
    .start() \
    .awaitTermination()

