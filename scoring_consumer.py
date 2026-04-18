from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def score_transaction(tx):
    score = 0
    rules = []
    
    amount = tx.get('amount', 0)
    category = tx.get('category', '')
    
    hour = tx.get('hour')
    if hour is None and 'timestamp' in tx:
        try:
            hour = int(tx['timestamp'].split('T')[1].split(':')[0])
        except:
            hour = 12

    if amount > 3000:
        score += 3
        rules.append('R1')
        
    if category == 'elektronika' and amount > 1500:
        score += 2
        rules.append('R2')
        
    if hour is not None and hour < 6:
        score += 2
        rules.append('R3')
        
    return score, rules

print("Uruchomiono system scoringowy. Oczekiwanie na transakcje...")

for message in consumer:
    tx = message.value
    score, rules = score_transaction(tx)
    
    if score >= 3:
        alert_payload = {
            'original_tx': tx,
            'score': score,
            'triggered_rules': rules,
            'alert_time': datetime.now().isoformat()
        }
        
        alert_producer.send('alerts', value=alert_payload)
        
        print(f"--- [ALERT] Wykryto podejrzaną transakcję! ---")
        print(f"ID: {tx['tx_id']} | Score: {score} | Reguły: {rules}")
        print(f"Kwota: {tx['amount']} PLN | Sklep: {tx['store']}")
        print("-" * 45)
    else:
        print(f"OK: {tx['tx_id']} (Score: {score})")

alert_producer.flush()
