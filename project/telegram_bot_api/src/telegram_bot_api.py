import telebot
import pandas as pd
import numpy as np
import pika
import json
import time

"""Переменные которые используются в нескольких функциях"""
global generate_data, x_gen, y_gen
generate_data = None
x_gen = None
y_gen = None 


# инициализация бота
bot = telebot.TeleBot(token='', threaded=False)


data = pd.read_csv("test.csv")
data.drop(columns="Unnamed: 0", inplace=True)

# /start
@bot.message_handler(commands=['start'])
def start_bot(msg):

    """Начало работы бота и описание команд"""
   
    message_line = ('Привет, я бот предсказатель результата!\n\n\n'
                    'Управление:\n'
                    '/start - запустить приветственное сообщение\n'
                    '/generate_data - сгенерировать данные для предсказания\n'
                    '/predict - предсказать результат\n'
                    )
    bot.send_message(msg.chat.id, message_line)


# /generate_data

@bot.message_handler(commands=['generate_data'])
def generate_data_bot(msg):

    """Генерация данных для демонстрации"""
   
    global generate_data, x_gen, y_gen
    generate_data = data.loc[np.random.randint(1000)].to_list()
    x_gen = generate_data[:-1]
    
    # проверка есть ли np.int и замена на обычный  int
    x_gen = [
    int(i) if isinstance(i, (np.integer, np.int64)) else i
    for i in x_gen
]

    print(x_gen)
    y_gen = generate_data[-1]
    message_line = (f'Сгенерированные данные{x_gen}\n\n\n'
                    '/generate_data - сгенерировать заново\n\n'
                    '/predict - предсказать результат\n\n'
                    '/truth - узнать настоящий результат для сгенерированных данных\n\n'
                    )
    bot.send_message(msg.chat.id, message_line)


# /truth  

@bot.message_handler(commands=['truth'])
def truth_bot(msg):

    """Показать правильный ответ"""

    global y_gen
    message_line = (f'Настоящий результат: {y_gen}'
                    )
    try:
        bot.send_message(msg.chat.id, message_line)
    except Exception as e:
        bot.send_message(msg.chat.id, "Ошибка, возможно вы забыли сгенерировать данные!")


# /predict

@bot.message_handler(commands=['predict'])
def truth_bot(msg):

    """Отправка данных в очередь и прием предсказанного ответа"""

    global x_gen
    if x_gen == None:
        bot.send_message(msg.chat.id, "Сгенерируйте данные")
    else:
      try:
          # Создаём подключение по адресу rabbitmq:
          connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
          channel = connection.channel()
          bot.send_message(msg.chat.id, "Подключился к высшим силам")
          # Объявляем очередь в telegram bot
          channel.queue_declare(queue='output_telegram_data')
          # Объявляем очередь из telegram bot
          channel.queue_declare(queue='input_telegram_data')

          bot.send_message(msg.chat.id, "Настраиваю магический шар...\n\n\n")

          # Публикуем сообщение в очередь features
          channel.basic_publish(exchange='',
                              routing_key='input_telegram_data',
                              body=json.dumps(x_gen))
          
          bot.send_message(msg.chat.id, "Космос слышит меня...\n\n\n")

          """Функция callback для обработки данных из очереди output_telegram_data"""
          def callback(ch, method, properties, body):
              print(f'Получено предсказание {body}')
              predict = json.loads(body)
              # Для красоты
              bot.send_message(msg.chat.id, "Спрашиваю у пророка...\n\n\n")
              time.sleep(1)
              # Изображение
              with open('image.png', 'rb') as photo:
                  bot.send_photo(msg.chat.id, photo)
              bot.send_message(msg.chat.id, "От судьбы не уйти!\n")
              bot.send_message(msg.chat.id, f"Целевое действие {predict}")
              # Останавливаем очередь после обработки сообщения
              ch.stop_consuming()
          
      

          # Извлекаем сообщение из очереди output_telegram_data
          channel.basic_consume(
              queue='output_telegram_data',
              on_message_callback=callback,
              auto_ack=True
          )

          # Запускаем режим ожидания прихода сообщений
          channel.start_consuming()
          connection.close()  # Закрываем соединение
      except:
          bot.send_message(msg.chat.id, "Не могу подключиться к очереди")




bot.polling()