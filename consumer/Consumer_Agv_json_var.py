from kafka import KafkaConsumer
import json
import redis


consumer = KafkaConsumer('Agv_Topic',
                         group_id='agv_group',
                         bootstrap_servers=['3.39.52.237:9092'])

redis_client = redis.StrictRedis(host='54.180.47.133', port=32535, db=0) # 2025.01.14 김석진 - host 및 port 변경 54.180.141.165:31724 -> 54.180.47.133:32535

for message in consumer:
# Kafka 메시지 출력
    try:
    # JSON 디코딩
        json_data = json.loads(message.value.decode('utf-8'))

        # JSON 데이터에서 키-값 쌍 추출
        for key, value in json_data.items():

            if json_data.get("Q001_X_number") :
                Q001_xpos = json_data["Q001_X_number"]
            if json_data.get("Q001_Y_number") :
                Q001_ypos = json_data["Q001_Y_number"]
            
            if json_data.get("Q002_X_number") :
                Q002_xpos = json_data["Q002_X_number"]
            if json_data.get("Q002_Y_number") :
                Q002_ypos = json_data["Q002_Y_number"]
            
            if json_data.get("Q003_X_number") :
                Q003_xpos = json_data["Q003_X_number"]
            if json_data.get("Q003_Y_number") :
                Q003_ypos = json_data["Q003_Y_number"]
            
            if json_data.get("Q004_X_number") :
                Q004_xpos = json_data["Q004_X_number"]
            if json_data.get("Q004_Y_number") :
                Q004_ypos = json_data["Q004_Y_number"]
            
        print("Q001_xpos : ", Q001_xpos)
        print("Q001_ypos : ", Q001_ypos)

        print("Q002_xpos : ", Q002_xpos)
        print("Q002_ypos : ", Q002_ypos)

        print("Q003_xpos : ", Q003_xpos)
        print("Q003_ypos : ", Q003_ypos)

        print("Q004_xpos : ", Q004_xpos)
        print("Q004_ypos : ", Q004_ypos)


    except UnicodeDecodeError as e:
        print(f"Error decoding message: {e}")

    
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
                "x_pos": 0,
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
                "x_pos": 0,
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
            },
            {
                "create_dt_": "2025-01-14 10:35:41.476000",
                "process_dt_": "2025-01-14 10:35:41.476000",
                "prefix_": "",
                "_object_id": "250114_Q003",
                "agv_id": "250114_Q003",
                "agv_name": "QRMain_03",
                "status": "RUN",
                "bat_lv": 90,
                "agv_type": "QR",
                "x_pos": 0,
                "y_pos": 0,
                "z_pos": 0,
                "area": "A_FL2",
                "alarm_id": "",
                "alarm_text": "",
                "matr_yn": "0",
                "run_td": 60892.685,
                "err_td": 106.265,
                "stop_td": 269875.65,
                "font_color": "#FFFFFF"
            },
            {
                "create_dt_": "2025-01-15 11:35:41.476000",
                "process_dt_": "2025-01-15 11:35:41.476000",
                "prefix_": "",
                "_object_id": "250115_Q004",
                "agv_id": "250115_Q004",
                "agv_name": "QRMain_04",
                "status": "RUN",
                "bat_lv": 90,
                "agv_type": "QR",
                "x_pos": 0,
                "y_pos": 0,
                "z_pos": 0,
                "area": "A_FL2",
                "alarm_id": "",
                "alarm_text": "",
                "matr_yn": "0",
                "run_td": 80892.158,
                "err_td": 306.269,
                "stop_td": 469875.18,
                "font_color": "#FFFFFF"
            }
        ]
    }
    """

    #세팅 된 JSON을 디코딩
    data_dict = json.loads(json_set_data)

    #디코딩 된 JSON에 컨슈밍된 데이터 세팅
    for agv_entry in  data_dict.get("agv", []):  
        if agv_entry.get("agv_id") == "230615_Q001":    
            agv_entry["x_pos"] = int(Q001_xpos)
            agv_entry["y_pos"] = int(Q001_ypos)
            # 최종 값이 범위를 벗어나지 않도록 제한
            agv_entry["x_pos"] = min(44, max(37, agv_entry["x_pos"]))
            agv_entry["y_pos"] = min(-81, max(-108, agv_entry["y_pos"]))

        if agv_entry.get("agv_id") == "230615_Q002":    
            agv_entry["x_pos"] = int(Q002_xpos)
            agv_entry["y_pos"] = int(Q002_ypos)
            # 최종 값이 범위를 벗어나지 않도록 제한
            agv_entry["x_pos"] = min(69, max(44, agv_entry["x_pos"]))
            agv_entry["y_pos"] = min(-114, max(-120, agv_entry["y_pos"]))

        if agv_entry.get("agv_id") == "250114_Q003":    
            agv_entry["x_pos"] = int(Q003_xpos)
            agv_entry["y_pos"] = int(Q003_ypos)
            # 최종 값이 범위를 벗어나지 않도록 제한
            agv_entry["x_pos"] = min(3, max(10, agv_entry["x_pos"]))
            agv_entry["y_pos"] = min(-81, max(-108, agv_entry["y_pos"]))

        if agv_entry.get("agv_id") == "250115_Q004":    
            agv_entry["x_pos"] = int(Q004_xpos)
            agv_entry["y_pos"] = int(Q004_ypos)
            # 최종 값이 범위를 벗어나지 않도록 제한
            agv_entry["x_pos"] = min(10, max(35, agv_entry["x_pos"]))
            agv_entry["y_pos"] = min(-114, max(-120, agv_entry["y_pos"]))


    # JSON 데이터 파싱하여 레디스 키와 값을 저장
    for key, value in data_dict.items():    
        redis_key = key
        redis_value = json.dumps(value) 
        
    # 레디스에 저장
    print(redis_key) 
    print(redis_value)
    redis_client.set(redis_key, redis_value)    

