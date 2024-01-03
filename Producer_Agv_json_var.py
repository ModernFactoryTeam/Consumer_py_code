import time
import json
from kafka import KafkaProducer

# Kafka 클러스터의 호스트 및 포트 정보를 설정합니다.
bootstrap_servers = ['15.164.226.233:9092']

# 토픽 이름을 설정합니다.
topicName = 'Agv_Topic'

# Kafka Producer 인스턴스를 생성합니다.
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

# 초기값 세팅
XY_number_json = """
     {
          "Q001_X_number" : 486,
          "Q001_Y_number" : 708,
          "Q001_Move" : "True",
          "Q002_X_number" : 486,
          "Q002_Y_number" : 630,
          "Q002_Move" : "True"
     }
     """

# 메시지를 생성하는 함수입니다.
def create_message():
    global XY_number_json
    data_dict = json.loads(XY_number_json)
   
    if data_dict["Q001_Move"] == "True":
          if data_dict["Q001_X_number"] < 506:
               if data_dict["Q001_X_number"] < 506 :
                    data_dict["Q001_X_number"] = data_dict["Q001_X_number"] + 1
          elif data_dict["Q001_X_number"] == 506 and data_dict["Q001_Y_number"] < 748 : 
               if data_dict["Q001_Y_number"] < 748 :
                    data_dict["Q001_Y_number"] = data_dict["Q001_Y_number"] + 1 
    elif data_dict["Q001_Move"] == "False":
          if data_dict["Q001_Y_number"] > 708  :
               if 708 < data_dict["Q001_Y_number"] : 
                    data_dict["Q001_Y_number"] = data_dict["Q001_Y_number"] - 1          
          elif data_dict["Q001_X_number"] > 486 and data_dict["Q001_Y_number"] == 708 :
               if 486 < data_dict["Q001_X_number"] :
                    data_dict["Q001_X_number"] = data_dict["Q001_X_number"] - 1
   
    if data_dict["Q001_X_number"] == 506 and data_dict["Q001_Y_number"] == 748 :
          data_dict["Q001_Move"] = "False"
    elif data_dict["Q001_X_number"] == 486 and data_dict["Q001_Y_number"] == 708 :
          data_dict["Q001_Move"] = "True"

   
    if data_dict["Q002_Move"] == "True":
          if data_dict["Q002_X_number"] < 520:
               if data_dict["Q002_X_number"] < 520 :
                    data_dict["Q002_X_number"] = data_dict["Q002_X_number"] + 1
          elif data_dict["Q002_X_number"] == 520 and data_dict["Q002_Y_number"] < 648 : 
               if data_dict["Q002_Y_number"] < 648 :
                    data_dict["Q002_Y_number"] = data_dict["Q002_Y_number"] + 1 
    elif data_dict["Q002_Move"] == "False":
          if data_dict["Q002_Y_number"] > 630  :
               if 630 < data_dict["Q002_Y_number"] : 
                    data_dict["Q002_Y_number"] = data_dict["Q002_Y_number"] - 1          
          elif data_dict["Q002_X_number"] > 486 and data_dict["Q002_Y_number"] == 630 :
               if 486 < data_dict["Q002_X_number"] :
                    data_dict["Q002_X_number"] = data_dict["Q002_X_number"] - 1
   
    if data_dict["Q002_X_number"] == 520 and data_dict["Q002_Y_number"] == 648 :
          data_dict["Q002_Move"] = "False"
    elif data_dict["Q002_X_number"] == 486 and data_dict["Q002_Y_number"] == 630 :
          data_dict["Q002_Move"] = "True"  
   
    XY_number_json = json.dumps(data_dict)

    message_2 = XY_number_json
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