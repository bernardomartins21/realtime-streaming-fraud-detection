from confluent_kafka import Producer
import random
import uuid
from datetime import datetime
import json
import time

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"USER_{random.randint(1, 100)}",
        "amount": round(random.uniform(5.0, 1000.0), 2),
        "timestamp": datetime.now().isoformat()
    }

# Configuração do Producer oficial da Confluent
producer_config = {
    'bootstrap.servers': '127.0.0.1:9092'
}

producer = Producer(producer_config)

if __name__ == "__main__":
    print("Produtor Confluent iniciado... Pressionar CTRL+C para encerrar.")
    try:
        while True:
            data = generate_transaction()
            account_key = data["account_id"]

            producer.produce(
                topic='transactions-raw',
                key=account_key.encode('utf-8'),
                value=json.dumps(data).encode('utf-8')
            )
            
            producer.poll(0)

            print(f"Evento enviado para [{account_key}]: R$ {data['amount']}")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n Encerrando o produtor...")
    finally:
        producer.flush()
        print(" Conexão encerrada com sucesso.")