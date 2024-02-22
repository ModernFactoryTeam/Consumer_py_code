from kafka import KafkaConsumer
import json
import redis


consumer = KafkaConsumer('Agv_Topic',
                         group_id='agv_group',
                         bootstrap_servers=['3.39.52.237:9092'])

redis_client = redis.StrictRedis(host='54.180.141.165', port=31724, db=0)

for message in consumer:
    # JSON 디코딩
    received_message = json.loads(message.value.decode('utf-8'))

    # Redis에 메시지 저장
    redis_client.set('agv', received_message)
    
    # 저장된 메시지 출력
    print(f"Received: {received_message}")