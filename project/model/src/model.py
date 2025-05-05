import pika
import pickle
import numpy as np
import json
import time
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
import math

class VisitDateTime(BaseEstimator, TransformerMixin):
    """Класс для преобразования данных visit_date и visit_time"""
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X['visit_date'] = pd.to_datetime(X['visit_date'], format="%Y-%m-%d")
        X['visit_time'] = pd.to_datetime(X['visit_time'], format="%H:%M:%S")
        return X
    
class MonthDayHour(BaseEstimator, TransformerMixin):
    """Класс для создания новых признаков из данных visit_date и visit_time"""
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # Создаем новые признаки (месяц, день недели, час)
        X['month'] = X['visit_date'].dt.month
        X['day_of_week'] = X['visit_date'].dt.dayofweek
        X['hour'] = (X['visit_time'].dt.hour + (X['visit_time'].dt.minute >= 30)) % 24
        return X
    
class TransformMonthDayHour(BaseEstimator, TransformerMixin):
    """Класс для преобразования признаков month, day_of_week, hour"""
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # Преобразует дни недели
        X['sin_day_of_week'] = X['day_of_week'].apply(math.sin)
        X['cos_day_of_week'] = X['day_of_week'].apply(math.cos)

        #  Эти признаки помогают учитывать сезонность (летом значения будут отличаться от зимних)
        X['sin_month'] = X['month'].apply(math.sin)
        X['cos_month'] = X['month'].apply(math.cos)

        # Это помогает моделям учитывать повторяющиеся суточные паттерны (например, 0:00 и 23:00 — близкие по времени).
        X['sin_hour'] = X['hour'].apply(math.sin)
        X['cos_hour'] = X['hour'].apply(math.cos)
        X['sin**2_hour'] = X['sin_hour'] * X['sin_hour']
        X['cos**2_hour'] = X['cos_hour'] * X['cos_hour']
        return X
    
class NormalizedMonthDayHour(BaseEstimator, TransformerMixin):
    """Класс для нормализации признаков month, day_of_week, hour"""
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # Преобразуем в диапазон [0, 2π] (нормализация)
        X['hour_sin'] = (X['hour'] / 23 * 2 * np.pi).apply(math.sin)
        X['hour_cos'] = (X['hour'] / 23 * 2 * np.pi).apply(math.cos)

        X['month_sin'] = ((X['month'] - 1) / 11 * 2 * np.pi).apply(math.sin)
        X['month_cos'] = ((X['month'] - 1) / 11 * 2 * np.pi).apply(math.cos)

        X['day_of_week_sin'] = ((X['day_of_week'] - 1) / 6 * 2 * np.pi).apply(math.sin)
        X['day_of_week_cos'] = ((X['day_of_week'] - 1) / 6 * 2 * np.pi).apply(math.cos)

        X['hour_sin**2'] = X['hour_sin'] * X['hour_sin']
        X['month_cos**2'] = X['month_cos'] * X['month_cos']
        X['day_of_week_cos**2'] = X['day_of_week_cos'] * X['day_of_week_cos']
        return X
    
class DropMonthDayHour(BaseEstimator, TransformerMixin):
    """Класс для удаления month, day_of_week, hour, visit_date, visit_time"""
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X.drop(['month', 'hour', 'day_of_week', 'visit_date', 'visit_time'], axis=1, inplace=True)
        return X
    
class FeaturesEncoder(BaseEstimator, TransformerMixin):
    """Класс для кодирования переменных"""
    def fit(self, X, y=None):
        df = pd.concat([X, y], axis=1)
        self.dct = dict()
        for col in ['utm_source', 'utm_medium', 'device_category', 'device_screen_resolution', 'device_browser', 'geo_country', 'geo_city']:
            # Вычисляем среднее целевой переменной для каждой категории
            self.dct[col] = df.groupby(col)['action'].mean().to_dict()
        return self

    def transform(self, X):
        for col, d in self.dct.items():
            if col in X.columns:
                X[col] = X[col].map(d)
        return X
    
class DropFeatures(BaseEstimator, TransformerMixin):
    """ Класс для преобразования данных visit_date и visit_time"""
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        lst = ['day_of_week_cos**2',
               'day_of_week_cos',
               'sin**2_hour',
               'hour_cos',
               'cos_day_of_week',
               'device_category',
               'day_of_week_sin',
               'sin_day_of_week']
        X.drop(columns=lst, inplace=True)
        return X
    

# Читаем файл с сериализованной моделью
with open('model.pkl', 'rb') as pkl_file:
    catboost_model = pickle.load(pkl_file)

while True:
    try:
        # Создаём подключение по адресу rabbitmq:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()

        # Объявляем очередь из telegram bot
        channel.queue_declare(queue='input_telegram_data')
        # Объявляем очередь из web
        channel.queue_declare(queue='input_web_data')
        # Объявляем очередь в telegram bot
        channel.queue_declare(queue='output_telegram_data')
        # Объявляем очередь в web
        channel.queue_declare(queue='output_web_data')

        def callback_telegram_bot(ch, method, properties, body):
            """Функция callback для обработки данных из очереди input_telegram_data"""
            # print(f'Из очереди {method.routing_key} получено значение {json.loads(body)}')
            features = json.loads(body)

            # Формирование набора данных для модели
            columns =["visit_date","visit_time","visit_number","utm_source","utm_medium","device_category","device_screen_resolution","device_browser","geo_country","geo_city"]
            arr = np.array(features).reshape(-1, len(columns))
            df = pd.DataFrame(arr, columns=columns)
            pred = int(catboost_model.predict(df)[0])
            
            # Публикуем ответ в очередь output_telegram_data
            channel.basic_publish(exchange='',
                            routing_key='output_telegram_data',
                            body=json.dumps(pred)) 
            
            print(f'Предсказание {pred} отправлено в очередь output_data')


        def callback_web(ch, method, properties, body):
            """Функция callback для обработки данных из очереди input_web_data"""
            #print(f'Из очереди {method.routing_key} получено значение {json.loads(body)}')
            features = json.loads(body)
            
            # Формирование набора данных для модели
            columns =["visit_date","visit_time","visit_number","utm_source","utm_medium","device_category","device_screen_resolution","device_browser","geo_country","geo_city"]
            arr = np.array(features).reshape(-1, len(columns))
            df = pd.DataFrame(arr, columns=columns)
            pred = int(catboost_model.predict(df)[0])
            
            # Публикуем ответ в очередь output_web_data
            channel.basic_publish(exchange='',
                            routing_key='output_web_data',
                            body=json.dumps(pred))
            
            print(f'Предсказание {pred} отправлено в очередь output_data')


        # Извлекаем сообщение из очереди input_telegram_data
        channel.basic_consume(
            queue='input_telegram_data',
            on_message_callback=callback_telegram_bot,
            auto_ack=True
        )
        
        # Извлекаем сообщение из очереди input_web_data
        channel.basic_consume(
            queue='input_web_data',
            on_message_callback=callback_web,
            auto_ack=True
        )

        print('...Ожидание сообщений, для выхода нажмите CTRL+C')

        # Запускаем режим ожидания прихода сообщений
        channel.start_consuming()
    except:
        print('Не удалось подключиться к очереди')