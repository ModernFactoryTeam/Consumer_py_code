from kafka import KafkaConsumer
import json
import redis


consumer = KafkaConsumer('Gauge_Topic',
                        #  group_id='testgroup',
                         bootstrap_servers=['3.37.127.75:9092'])

redis_client = redis.StrictRedis(host='54.180.141.165', port=31724, db=0)

for message in consumer:
# Kafka 메시지 출력
    print(f"Received Kafka Message: {message.value}")
    try:
        # 각 줄을 개행 문자로 분리하고, 숫자 부분을 추출
        input_strings = [int(line.split('-')[-1]) for line in message.value.decode('utf-8').split('\n')]

    except UnicodeDecodeError as e:
        print(f"Error decoding message: {e}")
 
    for input_data in input_strings:

        result = input_strings

        #json data setting
        json_set_data = """
        {
            "guage": { "shift": "1", 
                     "plan_qty": 70344, 
                     "acrs_qty": 42112, 
                     "target_qty": 41200, 
                     "defect_qty": 597, 
                     "mc_perform": "98.2%", 
                     "plan_tt": 96185, 
                     "actual_tt": 94525 
                    }
        }
        """
        data_dict = json.loads(json_set_data)



        # JSON 데이터 파싱하여 레디스 키와 값을 저장
        for key, value in data_dict.items():    
            redis_key = key
            redis_value = json.dumps(value) 
        # 레디스에 저장
        print(redis_key)    
        print(redis_value)
        redis_client.set(redis_key, redis_value)    

