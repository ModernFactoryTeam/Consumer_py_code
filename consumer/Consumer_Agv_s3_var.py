from kafka import KafkaConsumer
import json
import redis
import pandas as pd
import pyarrow as pa
from pyarrow import parquet

consumer = KafkaConsumer('Alarm-all',
                         group_id='Alarm',
                         bootstrap_servers=['3.37.127.75:9092'])

redis_client = redis.StrictRedis(host='54.180.141.165', port=31724, db=0)

for message in consumer:
# Kafka 메시지 출력
    try:
    # JSON 디코딩
        json_data = json.loads(message.value.decode('utf-8'))

        # JSON 데이터에서 키-값 쌍 추출
        # for key, value in json_data.items():

        #     if json_data.get("Q001_X_number") :
        #         Q001_xpos = json_data["Q001_X_number"]
        #     if json_data.get("Q001_Y_number") :
        #         Q001_ypos = json_data["Q001_Y_number"]
        #     if json_data.get("Q002_X_number") :
        #         Q002_xpos = json_data["Q002_X_number"]
        #     if json_data.get("Q002_Y_number") :
        #         Q002_ypos = json_data["Q002_Y_number"]
            
        # print("Q001_xpos : ", Q001_xpos)
        # print("Q001_ypos : ", Q001_ypos)
        # print("Q002_xpos : ", Q002_xpos)
        # print("Q002_ypos : ", Q002_ypos)


    except UnicodeDecodeError as e:
        print(f"Error decoding message: {e}")


def save_to_parquet(json_data, output_file):
    # JSON 데이터를 DataFrame으로 변환
    df = pd.DataFrame(json_data)

    # 스키마 정보 가져오기
    parquet_schema = pa.Schema.from_pandas(df)

    # 데이터프레임을 Arrow 테이블로 변환
    table = pa.Table.from_pandas(df, schema=parquet_schema)

    # Parquet 파일로 저장
    parquet.write_table(table, output_file)


    #세팅 된 JSON을 디코딩
#    data_dict = json.loads(json_set_data)

    #디코딩 된 JSON에 컨슈밍된 데이터 세팅
    # for agv_entry in  json_data.get("agv", []):  
    #     print("json : "+json_data)
        # if agv_entry.get("agv_id") == "230615_Q001":    
        #     agv_entry["x_pos"] = int(Q001_xpos)
        #     agv_entry["y_pos"] = int(Q001_ypos)
        #     # 최종 값이 범위를 벗어나지 않도록 제한
        #     agv_entry["x_pos"] = min(44, max(37, agv_entry["x_pos"]))
        #     agv_entry["y_pos"] = min(-81, max(-108, agv_entry["y_pos"]))

        # if agv_entry.get("agv_id") == "230615_Q002":    
        #     agv_entry["x_pos"] = int(Q002_xpos)
        #     agv_entry["y_pos"] = int(Q002_ypos)
        #     # 최종 값이 범위를 벗어나지 않도록 제한
        #     agv_entry["x_pos"] = min(69, max(44, agv_entry["x_pos"]))
        #     agv_entry["y_pos"] = min(-114, max(-120, agv_entry["y_pos"]))


    # JSON 데이터 파싱하여 레디스 키와 값을 저장
    print(json_data.items())
    for key, value in json_data.items():    
        redis_key = key
        redis_value = json.dumps(value) 
        
    # 레디스에 저장
    # print(redis_key) 
    # print(redis_value)
    #redis_client.set(redis_key, redis_value)    

