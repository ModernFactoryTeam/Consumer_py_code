from kafka import KafkaConsumer
import json
import redis


consumer = KafkaConsumer('Agv_Topic',
                         group_id='agv_group',
                         bootstrap_servers=['3.39.52.237:9092'])

redis_client = redis.StrictRedis(host='54.180.47.133', port=32535, db=0)

for message in consumer:
    # JSON 디코딩
    received_message = json.loads(message.value.decode('utf-8'))

    #key,value 분류
    for key, value in received_message.items():
        redis_key = key
        redis_value = json.dumps(value) 
   
    # Redis에 메시지 저장
    print(redis_key) 
    print(redis_value)    
    redis_client.set(redis_key, redis_value)


    # 저장된 메시지 출력
    #print(f"Received: {received_message}")