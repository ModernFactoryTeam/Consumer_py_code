from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.DEBUG)

producer = KafkaProducer(bootstrap_servers=['3.39.52.237:9092'])

producer.send('Agv_Topic', b'Test message')  # 테스트 메시지 전송
print("Message sent!")