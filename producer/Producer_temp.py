import time 
import json
from kafka import KafkaProducer

# Kafka 클러스터의 호스트 및 포트 정보를 설정합니다.
bootstrap_servers = ['3.39.52.237:9092']

# 토픽 이름을 설정합니다.
topicName = 'Agv_Topic'

# Kafka Producer 인스턴스를 생성합니다.
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)


# 초기값 세팅
current_wp_number = 1
wp_sequence = [1, 2, 5, 6, 7, 9]  # WP list 생성
cycle_index = 0  # WP 인덱스 추적

# BurdenType 기본값
default_burden_type = "NONE"
 
# 메시지를 생성하는 함수입니다.
def create_message():
    global current_wp_number
    global cycle_index
    
    # 순환 시퀀스에 따라 current_wp_number 값을 설정합니다.
    current_wp_number = wp_sequence[cycle_index]
    
    # "BurdenType"의 값을 설정합니다.
    if current_wp_number == 2:
        burden_type = "METANET_BOX"
    elif current_wp_number == 6:
        burden_type = "OMT_BOX"
    else:
        burden_type = default_burden_type
    
    agv_Line1_Agv1 = {
        "WPCode": f"Line1_WP ({current_wp_number})",
        "AGVName": "Line1_AGV_01",
        "BurdenType": burden_type
    }
    
    agv_Line1_Agv2 = {
        "WPCode": f"Line1_WP ({current_wp_number})",
        "AGVName": "Line1_AGV_02",
        "BurdenType": burden_type
    }
    
    agv_Line2_Agv3 = {
        "WPCode": f"Line2_WP ({current_wp_number})",
        "AGVName": "Line2_AGV_03",
        "BurdenType": burden_type
    }
    
    agv_Line2_Agv4 = {
        "WPCode": f"Line2_WP ({current_wp_number})",
        "AGVName": "Line2_AGV_04",
        "BurdenType": burden_type
    }
    
    # WP 인덱스 갱신
    cycle_index = (cycle_index + 1) % len(wp_sequence)
    
    # 전체 키 'agv' 생성
    message = {'agv': [agv_Line1_Agv1, agv_Line1_Agv2, agv_Line2_Agv3, agv_Line2_Agv4]}
    
    message_2 = json.dumps(message)
    
    return message_2


# 메시지를 보내는 함수입니다.
def send_message(message):
     # 메시지 보내기 등의 작업 수행
     print('Sent message :'+ message)
     value = message.encode('UTF-8')
     producer.send(topicName, value)

# 메시지를 보냅니다.
send_message(create_message())


while True:
     # 메시지를 보냅니다.
     time.sleep(5)
     send_message(create_message())
