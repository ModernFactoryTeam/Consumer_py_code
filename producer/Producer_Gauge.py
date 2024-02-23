import time
import random
from kafka import KafkaProducer

# Kafka 클러스터의 호스트 및 포트 정보를 설정합니다.
bootstrap_servers = ['3.39.52.237:9092']

# 토픽 이름을 설정합니다.
topicName = 'Gauge_Topic'

# Kafka Producer 인스턴스를 생성합니다.
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

# 메시지를 생성하는 함수입니다.
def create_message():
    random_number = random.randint(486, 748)
    message = str(random_number)
    return message

# 메시지를 보내는 함수입니다.
def send_message(message):
        # 메시지 보내기 등의 작업 수행
        print('Sent message :', message)
        value=message.encode('utf-8')
        producer.send(topicName, value)
    
# 메시지를 보냅니다.
send_message(create_message())

# 10초마다 메시지를 보냅니다.
while True:
    time.sleep(5)
    send_message(create_message())

