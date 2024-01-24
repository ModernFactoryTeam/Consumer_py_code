from kafka import KafkaConsumer
import json
import redis


consumer = KafkaConsumer('Agv_Topic',
                        #  group_id='testgroup',
                         bootstrap_servers=['15.164.226.233:9092'])

redis_client = redis.StrictRedis(host='52.78.31.128', port=31724, db=0)

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
            "agv": [
                {
                    "create_dt_": "2023-12-20 13:36:41.476000",
                    "process_dt_": "2023-12-20 13:36:42.310999",
                    "prefix_": "",
                    "_object_id": "230615_Q001",
                    "agv_id": "230615_Q001",
                    "agv_name": "QRMain_01",
                    "status": "RUN",
                    "bat_lv": 82,
                    "agv_type": "QR",
                    "x_pos": 2.354622,
                    "y_pos": 0,
                    "z_pos": 0,
                    "area": "A_FL2",
                    "alarm_id": "",
                    "alarm_text": "",
                    "matr_yn": "0",
                    "run_td": 330630.939,
                    "err_td": 3106.483,
                    "stop_td": 461193.153,
                    "font_color": "#FFFFFF"
                },
                {
                    "create_dt_": "2023-12-20 13:36:41.476000",
                    "process_dt_": "2023-12-20 13:36:42.313108",
                    "prefix_": "",
                    "_object_id": "230615_Q002",
                    "agv_id": "230615_Q002",
                    "agv_name": "QRMain_02",
                    "status": "RUN",
                    "bat_lv": 90,
                    "agv_type": "QR",
                    "x_pos": 125.795056,
                    "y_pos": 0,
                    "z_pos": 0,
                    "area": "A_FL2",
                    "alarm_id": "",
                    "alarm_text": "",
                    "matr_yn": "0",
                    "run_td": 70892.673,
                    "err_td": 206.816,
                    "stop_td": 369875.28,
                    "font_color": "#FFFFFF"
                }
            ]
        }
        """
        data_dict = json.loads(json_set_data)
        for agv_entry in  data_dict.get("agv", []):  
            if agv_entry.get("agv_id") == "230615_Q001":    
                agv_entry["x_pos"] = result[0]
                agv_entry["y_pos"] = result[0]
                # 최종 값이 범위를 벗어나지 않도록 제한
                agv_entry["x_pos"] = min(506, max(486, agv_entry["x_pos"]))
                agv_entry["y_pos"] = min(748, max(708, agv_entry["y_pos"]))

            if agv_entry.get("agv_id") == "230615_Q002":    
                agv_entry["x_pos"] = result[0]
                agv_entry["y_pos"] = result[0]
                # 최종 값이 범위를 벗어나지 않도록 제한
                agv_entry["x_pos"] = min(520, max(486, agv_entry["x_pos"]))
                agv_entry["y_pos"] = min(648, max(630, agv_entry["y_pos"]))


        # JSON 데이터 파싱하여 레디스 키와 값을 저장
        for key, value in data_dict.items():    
            redis_key = key
            redis_value = json.dumps(value) 
        # 레디스에 저장
        # print(redis_value)
        redis_client.set(redis_key, redis_value)    

