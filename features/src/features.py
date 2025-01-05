import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
import time
from datetime import datetime

# Загружаем датасет о диабете
X, y = load_diabetes(return_X_y=True)

# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    try:

        # Формируем случайный индекс строки
        random_row = np.random.randint(0, X.shape[0]-1)
        time.sleep(10)
 
        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()

        # уникальный идентификатор
        message_id = datetime.timestamp(datetime.now())
 

        # Создаём очередь y_true
        channel.queue_declare(queue='y_true')

        # Формируем сообщение
        message_y_true = {
            'id': message_id,
            'body': y[random_row]
            }

        # Публикуем сообщение в очередь y_true
        channel.basic_publish(exchange='',
                            routing_key='y_true',
                            body=json.dumps(message_y_true))
                            #body=json.dumps(y[random_row]))
        print('Сообщение с правильным ответом отправлено в очередь')


        # Создаём очередь features
        channel.queue_declare(queue='features')

        # Формируем сообщение
        message_features = {
            'id': message_id,
            'body': list(X[random_row])
            }

        # Публикуем сообщение в очередь features
        channel.basic_publish(exchange='',
                            routing_key='features',
                            body=json.dumps(message_features))
                            #body=json.dumps(list(X[random_row])))
        print('Сообщение с вектором признаков отправлено в очередь')
 
         # Создаём очередь plot
        channel.queue_declare(queue='plot')

        # Формируем сообщение
        message_plot = {
            'id': message_id,
            'body': 0
            }

        # Публикуем сообщение в очередь plot
        channel.basic_publish(exchange='',
                            routing_key='plot',
                            body=json.dumps(message_plot))
        print('Сообщение plot отправлено в очередь')

        # Закрываем подключение
        connection.close()
        
    except:
        print('Не удалось подключиться к очереди')
        