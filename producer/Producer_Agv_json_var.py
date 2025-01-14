import time 
import json
from kafka import KafkaProducer

# Kafka 클러스터의 호스트 및 포트 정보를 설정합니다.
bootstrap_servers = ['3.39.52.237:9092']

# 토픽 이름을 설정합니다.
topicName = 'Agv_Topic'

# Kafka Producer 인스턴스를 생성합니다.
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

# 초기값 세팅
XY_number_json = """
     {
          "Q001_X_number" : 44,
          "Q001_Y_number" : -108,
          "Q001_Move" : "True",
          "Q002_X_number" : 69,
          "Q002_Y_number" : -120,
          "Q002_Move" : "True",
          "Q003_X_number" : 10,
          "Q003_Y_number" : -108,
          "Q003_Move" : "True",
          "Q004_X_number" : 35,
          "Q004_Y_number" : -120,
          "Q004_Move" : "True"
     }
     """

# 메시지를 생성
def create_message():
     global XY_number_json
     data_dict = json.loads(XY_number_json)
   
     # AGV 1번: Q001_Move (True: 전진, False: 초기 자리로)
     if data_dict["Q001_Move"] == "True":
          if data_dict["Q001_Y_number"] < -81 : 
               data_dict["Q001_Y_number"] = data_dict["Q001_Y_number"] + 1
          elif data_dict["Q001_Y_number"] == -81 and data_dict["Q001_X_number"] > 37:
               data_dict["Q001_X_number"] = data_dict["Q001_X_number"] - 1           


     elif data_dict["Q001_Move"] == "False":
          if data_dict["Q001_Y_number"] > -108 :
               data_dict["Q001_Y_number"] = data_dict["Q001_Y_number"] - 1
          elif data_dict["Q001_Y_number"] == -108 and data_dict["Q001_X_number"] < 44 :
               data_dict["Q001_X_number"] = data_dict["Q001_X_number"] + 1          

     if data_dict["Q001_X_number"] == 37 and data_dict["Q001_Y_number"] == -81 :
          data_dict["Q001_Move"] = "False"
     elif data_dict["Q001_X_number"] == 44 and data_dict["Q001_Y_number"] == -108 :
          data_dict["Q001_Move"] = "True"

     # AGV 2번: Q002_Move (True: 전진, False: 초기 자리로)
     if data_dict["Q002_Move"] == "True":
          if data_dict["Q002_Y_number"] < -114 :
               data_dict["Q002_Y_number"] = data_dict["Q002_Y_number"] + 1 
          elif data_dict["Q002_Y_number"] == -114 and data_dict["Q002_X_number"] > 44 :
               data_dict["Q002_X_number"] = data_dict["Q002_X_number"] - 1   


     elif data_dict["Q002_Move"] == "False":
          if data_dict["Q002_Y_number"] > -120  :
               data_dict["Q002_Y_number"] = data_dict["Q002_Y_number"] - 1          
          elif data_dict["Q002_Y_number"] == -120 and data_dict["Q002_X_number"] < 69:
               data_dict["Q002_X_number"] = data_dict["Q002_X_number"] + 1

     if data_dict["Q002_X_number"] == 44 and data_dict["Q002_Y_number"] == -114 :
          data_dict["Q002_Move"] = "False"
     elif data_dict["Q002_X_number"] == 69 and data_dict["Q002_Y_number"] == -120 :
          data_dict["Q002_Move"] = "True"  

     # AGV 3번: Q003_Move (True: 전진, False: 초기 자리로)
     if data_dict["Q003_Move"] == "True":
          if data_dict["Q003_Y_number"] < -81 : 
               data_dict["Q003_Y_number"] = data_dict["Q003_Y_number"] + 1
          elif data_dict["Q003_Y_number"] == -81 and data_dict["Q003_X_number"] > 3:
               data_dict["Q003_X_number"] = data_dict["Q003_X_number"] - 1           


     elif data_dict["Q003_Move"] == "False":
          if data_dict["Q003_Y_number"] > -108 :
               data_dict["Q003_Y_number"] = data_dict["Q003_Y_number"] - 1
          elif data_dict["Q003_Y_number"] == -108 and data_dict["Q003_X_number"] < 10 :
               data_dict["Q003_X_number"] = data_dict["Q003_X_number"] + 1          

     if data_dict["Q003_X_number"] == 3 and data_dict["Q003_Y_number"] == -81 :
          data_dict["Q003_Move"] = "False"
     elif data_dict["Q003_X_number"] == 10 and data_dict["Q003_Y_number"] == -108 :
          data_dict["Q003_Move"] = "True"

     # AGV 4번: Q004_Move (True: 전진, False: 초기 자리로)
     if data_dict["Q004_Move"] == "True":
          if data_dict["Q004_Y_number"] < -114 : 
               data_dict["Q004_Y_number"] = data_dict["Q004_Y_number"] + 1
          elif data_dict["Q004_Y_number"] == -114 and data_dict["Q004_X_number"] > 10:
               data_dict["Q004_X_number"] = data_dict["Q004_X_number"] - 1           


     elif data_dict["Q004_Move"] == "False":
          if data_dict["Q004_Y_number"] > -120 :
               data_dict["Q004_Y_number"] = data_dict["Q004_Y_number"] - 1
          elif data_dict["Q004_Y_number"] == -120 and data_dict["Q004_X_number"] < 35 :
               data_dict["Q004_X_number"] = data_dict["Q004_X_number"] + 1          

     if data_dict["Q004_X_number"] == 10 and data_dict["Q004_Y_number"] == -114 :
          data_dict["Q004_Move"] = "False"
     elif data_dict["Q004_X_number"] == 35 and data_dict["Q004_Y_number"] == -120 :
          data_dict["Q004_Move"] = "True"


     XY_number_json = json.dumps(data_dict)

     message = XY_number_json
     return message



# 메시지 전송 함수수
def send_message(message):
     print('Sent message :'+ message)
     value = message.encode('UTF-8')
     producer.send(topicName, value)

send_message(create_message())


while True:
     # 1초 텀으로 메시지 전송
     time.sleep(1)
     send_message(create_message())
