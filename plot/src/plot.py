import pika
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import os
import json
import time

def png():
    file_path = './logs/metric_log.csv'
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        sns.histplot(df['absolute_error'], kde=True,  color="pink")
        plt.savefig('./logs/error_distribution.png')
    else:
        print('Не найден файл metric_log.csv')
        return
    
try:
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
     # Объявляем очередь
    channel.queue_declare(queue='plot')
    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        print(f'Из очереди {method.routing_key} получено значение {json.loads(body)}')
        png()
    
    # Извлекаем сообщение из очереди
    channel.basic_consume(
        queue='plot',
        on_message_callback=callback,
        auto_ack=True
    )
    # Запускаем режим ожидания прихода сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()
except:
    print('Не удалось подключиться к очереди')