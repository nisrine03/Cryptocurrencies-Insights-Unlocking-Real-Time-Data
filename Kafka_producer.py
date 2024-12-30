import requests
import json
from confluent_kafka import Producer, AdminClient, NewTopic
import time

# Binance API setup
CRYPTO_SYMBOLS = ["BTCUSDT", "ETHUSDT"]  # Liste des paires de crypto-monnaies
BASE_URL = ""  # your Api

# Kafka setup
KAFKA_BROKER = 'localhost:9092'  
KAFKA_TOPIC = 'cryptocurrency'  

# Configure Kafka producer
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'python-producer'
}
producer = Producer(conf)

# Configure Kafka admin client
admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})

def create_kafka_topic(topic_name):
    # Vérifie si le topic existe déjà
    existing_topics = admin_client.list_topics(timeout=10).topics
    if topic_name not in existing_topics:
        print(f"Topic '{topic_name}' n'existe pas. Création en cours...")
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        try:
            admin_client.create_topics([new_topic])
            print(f"Topic '{topic_name}' créé avec succès.")
        except Exception as e:
            print(f"Erreur lors de la création du topic '{topic_name}': {e}")
    else:
        print(f"Topic '{topic_name}' existe déjà.")

def fetch_crypto_data():
    result = []
    for symbol in CRYPTO_SYMBOLS:
        url = f'{BASE_URL}?symbol={symbol}'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            result.append({
                "symbol": data['symbol'],
                "price": data['lastPrice'],  
                "open": data['openPrice'],    
                "high": data['highPrice'],   
                "low": data['lowPrice'],      
                "close": data['lastPrice'],    
                "volume": data['volume'],      
                "timestamp": time.time()       
            })
        else:
            print(f"Error fetching data for {symbol}")
    return result if result else None

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def stream_crypto_data():
    create_kafka_topic(KAFKA_TOPIC)  # Création automatique du topic
    while True:
        data = fetch_crypto_data()
        if data:
            # Envoi des données à Kafka pour chaque crypto-monnaie
            for entry in data:
                producer.produce(KAFKA_TOPIC, key='crypto', value=json.dumps(entry), callback=delivery_report)
                print(f"Sent data: {entry}")
        producer.flush()  
        time.sleep(1)  

if __name__ == "__main__":
    stream_crypto_data()

