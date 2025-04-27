from fastapi import FastAPI, Form
from fastapi.responses import FileResponse
from datetime import datetime
import pika
import json
# web принимает сообщение и пуяет его в очередь
# модель его обрабатывает и возвращает обратно
# это сообщение нужно выплюнуть обратно в web

app = FastAPI()

# Отображение формы для ввода
@app.get("/")
def root():
    return FileResponse("index.html")

# Отправка и прием предсказания по нажатию кнопки
@app.post("/predict")
def predict (features: list =Form()):

    # Переменная для предсказаний и ошибок
    у_pred = None
    errors = []

    # Проверки для date и time
    try:
        datetime.strptime(features[0], "%Y-%m-%d")
    except:
        errors.append(f'visit_date')
    try:
        datetime.strptime(features[1], "%H:%M:%S")
    except:
        errors.append(f'visit_time')

    if len(errors) != 0:
        return f"Incorrect '{(', ').join(errors)}' format! Please fix it and try again"


    try:
        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()
        # Объявляем очередь из web
        channel.queue_declare(queue='input_web_data')

        # Объявляем очередь в web
        channel.queue_declare(queue='output_web_data')

        # Публикуем сообщение в очередь input_web_data
        channel.basic_publish(exchange='',
                          routing_key='input_web_data',
                          body=json.dumps(features))


        def callback(ch, method, properties, body):
            """Функция для получения данных из очередь output_web_data"""
            global y_pred
            y_pred = json.loads(body)
            print(f'Из очереди {method.routing_key} получено значение {y_pred}')
            ch.stop_consuming()
            return 


        # Извлекаем сообщение из очереди output_web_data
        # on_message_callback показывает, какую функцию вызвать при получении сообщения
        channel.basic_consume(
            queue='output_web_data',
            on_message_callback=callback,
            auto_ack=True
        )
        channel.start_consuming()

    except:
        return f'Error'

    return f'The probability of the target action being performed by the user - {y_pred}'

