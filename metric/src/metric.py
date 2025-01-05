import pika
import json
import pandas as pd
import os.path

def logging_csv(id, value, queuee):
    file_path = './logs/metric_log.csv'
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
    else:
        df = pd.DataFrame(columns=['id', 'y_true', 'y_pred', 'absolute_error'])

    # Проверка наличия записи
    id_ = df['id'] == id
    if any(id_):
        if queuee == 'y_true':
            df.loc[id_, 'y_true'] = value
            df.loc[id_, 'absolute_error'] = abs(value - df.loc[id_, 'y_pred'])
        elif queuee == 'y_pred':
            df.loc[id_, 'y_pred'] = value
            df.loc[id_, 'absolute_error'] = abs(df.loc[id_, 'y_true']-value)
       # else:
            #print(f'Очередь {queuee}')
    else:
        df.loc[len(df)] = [
            id,
            value if queuee == 'y_true' else None,
            value if queuee == 'y_pred' else None,
            None
        ]

    df.to_csv(file_path, index=False)
 
try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
   
    # Объявляем очередь y_true
    channel.queue_declare(queue='y_true')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')
 
    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        answer_string = f'Из очереди {method.routing_key} получено значение {json.loads(body)}'
        with open('./logs/labels_log.txt', 'a') as log:
            log.write(answer_string +'\n')
        
        response    = json.loads(body.decode('utf-8'))
        response_y  = response['body']
        response_id = response['id']
        #logging_csv(response_id, response_y, method.routing_key)
        logging_csv(response_id, response_y, 'y_true' if method.routing_key == 'y_true' else 'y_pred')

  
 
    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(
        queue='y_true',
        on_message_callback=callback,
        auto_ack=True
    )
    # Извлекаем сообщение из очереди y_pred
    channel.basic_consume(
        queue='y_pred',
        on_message_callback=callback,
        auto_ack=True
    )
 
    # Запускаем режим ожидания прихода сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()
except:
    print('Не удалось подключиться к очереди')