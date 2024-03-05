import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import date
from datetime import datetime, timedelta
import io
import sys
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'password',
    'user': 'user',
    'database': 'simulator_20240120'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-ananev',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 2, 8),
}

schedule_interval = '*/15 * * * *' # каждый день - каждые  15 минут

my_token = 'token'
bot = telegram.Bot(token=my_token)

chat_id = 00000000000
group_chat_id = -00000000000
sigma = 3 
iqr=1.5
days=7
znak = ''


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def edwan70_alerts():
    @task()
    def extract_data():
        # Формирование запроса к базе данных
        query_1 = '''SELECT *
                    FROM
                        (select toStartOfFifteenMinutes(time) as timestamp,
                            toDate(time) as date,
                            formatDateTime(timestamp, '%R') as hm,
                            count(distinct user_id) as users_feed,
                            countIf(user_id, action='like') as likes,
                            countIf(user_id, action='view') as views,
                            likes/views * 100 as CTR
                        from simulator_20240120.feed_actions
                        where timestamp >=  today() - 7 and timestamp < toStartOfFifteenMinutes(now())
                        group by timestamp, date, hm) t1
                    FULL JOIN
                        (select toStartOfFifteenMinutes(time) as timestamp,
                            toDate(time) as date,
                            formatDateTime(timestamp, '%R') as hm,
                            count(distinct user_id) as users_messenger,
                            count(user_id) as messages,
                            messages/users_messenger as messages_per_user
                        from simulator_20240120.message_actions
                        where timestamp >=  today() - 7 and timestamp < toStartOfFifteenMinutes(now())
                        group by timestamp, date, hm
                        order by toDate(time) DESC) t2 using timestamp, date, hm
                    ORDER BY timestamp DESC'''
    
        # Датафрейм для проверки аномалий
        df = ph.read_clickhouse(query_1, connection=connection)
        return df    
    
    @task()
    def run_alerts(df, chat_id, sigma, iqr):
    
        """
        Функция run_alerts выполняет запрос на проверку 
        метрик на аномалии, а также формирует информацию для отчета в telegram
    
        Параметры
        ---------
        df - датафрейм
        chat - chat_id telegram для отправки сообщения
        sigma - коэффициент сигмы
        iqr - коэффициент межквартильного размаха
        """
        def check_anomaly(df, metric, sigma, iqr, days):
            """
            функция check_anomaly проверяет значения на аномальность посредством
            сравнения текущего значения метрики со средненедельным показателем в 15 минутном интервале.

            Параметры
            ---------
            df - датафрейм
            metric - метрика, кототорая проверяется
            sigma - коэффициент сигмы
            iqr - коэффициент межквартильного размаха
            days - число предыдущих дней, которые идут для определения аномалии.

            Функция возвращает: 
            is_alert - запуск оповещения, 1 или 0
            current_value - текущее значение метрики
            diff - отклонение текущего значения от среднего за N дней в это же время
            """

            # найдем список значений метрики за текущий и предыдущие дни
            list_of_value = []
            for n in range(0, days + 1):
                # достаем максимальную 15-минутку из датафрейма - ту, которую будем проверять на аномальность
                current_timestamp = df['timestamp'].max() 
                n_days_ago_timestamp = current_timestamp - pd.DateOffset(days=n)
                # достаем из датафрейма значение метрики в максимальную 15-минутку
                n_days_ago_value = df[df['timestamp'] == n_days_ago_timestamp][metric].iloc[0]         
                list_of_value.append(round(n_days_ago_value, 2))
                print(n_days_ago_timestamp, n_days_ago_value)

     
            # значение метрики в текущую 15-минутку, удаляем из общего списка
            current_value = list_of_value.pop(0)
            # рассчитываем значение от которого будет определяться вызов алерта
            mean_value = np.mean(list_of_value)
            print('last_week_value', list_of_value, '\n',
                  'current_value', current_value, '\n', 
                  'mean_value', round(mean_value, 2))

            # вычисляем отклонение
            if current_value <= mean_value:
                diff = abs(current_value / mean_value - 1)
            else:
                diff = abs(mean_value / current_value - 1)
            print(' diff', round(diff, 2))

    
            # Вариант 1. Определяем верхние и нижние границы по правилу N сигм
            low_sigm = mean_value - sigma * np.std(list_of_value)
            upper_sigm = mean_value + sigma * np.std(list_of_value)
    
    
            # Вариант 2. Определяем верхние и нижние границы N * межквартильного размаха
            quant_25 = np.quantile(list_of_value, 0.25)
            quant_75 = np.quantile(list_of_value, 0.75)
            low_quant = quant_25 - iqr * (quant_75 - quant_25)
            upper_quant = quant_75 + iqr * (quant_75 - quant_25)
            
            # определим в какую сторону отклонение
            if current_value < (low_sigm or low_quant):
                znak = '-'
            elif current_value > (upper_sigm or upper_quant):
                znak = '+'
            else:
                znak = ''
    
            # Проверяем вызов алерта
            ''' Настроим аллерт на одновременную проверку границ по 3-м сигмам и 1,5 межквартильным размахам.'''
            if (current_value < (low_sigm or low_quant)
                or current_value > (upper_sigm or upper_quant)):
                is_alert = 1
            else:
                is_alert = 0
            print('is_alert =', is_alert, 'границы алерта 3-х сигм: ', round(low_sigm, 2), round(upper_sigm, 2))
            print('is_alert =', is_alert, 'границы алерта 1.5*iqr: ', round(low_quant, 2), round(upper_quant, 2))
            print('-'*60)
            return is_alert, current_value, diff, znak
    
        # Проверка группы метрик на анамалии
        metrics = df.columns[3:]
        for metric in metrics:
            print('Метрика: ', metric)
            # проверяем метрику на аномальность с помощью функции check_anomaly()
            is_alert, current_value, diff, znak = check_anomaly(df=df, metric=metric, sigma=sigma, iqr=iqr, days = days)
            if is_alert == 1:
            
                def send_msg(metric=metric, current_value=current_value, diff=diff, znak=znak, chat_id = group_chat_id):
                    date_to_show = df['date'].max().strftime('%d-%m-%Y')
                    hour_min = df['hm'].iloc[0]
                    links={
                    'users_feed' : 'https://redash.lab.karpov.courses/queries/45453#92474',
                    'likes' : 'https://redash.lab.karpov.courses/queries/45453#92472',
                    'views' : 'https://redash.lab.karpov.courses/queries/45453#92473',
                    'CTR' : 'https://redash.lab.karpov.courses/queries/45453#92476',
                    'users_messenger' : 'https://redash.lab.karpov.courses/queries/45453#92475',
                    'messages' : 'https://redash.lab.karpov.courses/queries/45453#92478',
                    'messages_per_user' : 'https://redash.lab.karpov.courses/queries/45453#92477'
                    }
                    link = links.get(f'{metric}', 'Такого ключа в словаре нет')

                    # Формируем сообщение для отправки
                    if metric in ['CTR', 'messages_per_user']:
                        value=f'{current_value:.2f}'
                    else:
                        value=f'{current_value:.0f}'
                    msg = f'''
Отчет об аномалии за *{date_to_show} {hour_min}*:
Метрика *{metric}*:
 - текущее значение: *{value}*
 - отклонение от скользящего среднего за неделю: *{znak}{diff:.2%}*
Посмотреть в Redash: {link}
    '''
                    bot.sendMessage(chat_id=group_chat_id, text=msg, parse_mode="Markdown")

                # отправляем алерт
                send_msg(metric=metric, current_value=current_value, diff=diff, znak=znak, chat_id = group_chat_id)
            
                def get_plot_1(df=df, sigma=sigma, metric=metric):
                    # Строим график
                    plt.figure(figsize=(14, 5)) # задаем размер графика
                    plt.tight_layout() # плотная компоновка

            
                    ax1 = sns.lineplot(data=df[df.date == df.date.unique()[0]].sort_values(by=['date', 'hm']), 
                              x='hm', 
                              y=f'{metric}',
                              label = '{metric}, {day}'.format(metric = metric,
                              day = df['date'].dt.date[0]))
                    # разряжаем подписи по оси X
                    for ind, label in enumerate(ax1.get_xticklabels()):
                        if ind % 15 == 0:
                            label.set_visible(True)
                        else:
                            label.set_visible(False)
                    ax1.set(xlabel='time') # задаем имя оси Х
                    ax1.set(ylabel=f'{metric}') # задаем имя оси У
                    ax1.set_title(f'{metric}'.format(metric)) # задаем заголовок графика
                    ax1.set(ylim=(0, None)) # задаем лимит для оси У
                    # формируем файловый объект
                    plot_object = io.BytesIO()
                    ax1.figure.savefig(plot_object)
                    plot_object.seek(0)
                    plot_object.name = f'{metric}.png'.format(metric)
                    plt.close()
                    bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)

                get_plot_1(df=df, sigma=sigma, metric=metric)
            
                # при сигма 1/ 2/ 3 доверительный интервал соответственно равен 68,3% / 95,5% / 99,7%
                def get_plot_7(df=df, sigma=sigma, metric=metric):
                    # Строим график
                    plt.figure(figsize=(14, 5)) # задаем размер графика
                    plt.tight_layout() # плотная компоновка
                    # визуализация среднего значения и доверительного интервала за предыдущую неделю
                    ax2 = sns.lineplot(data=df[df.date < df.date.unique()[0]].sort_values(by=['date', 'hm']), 
                              x='hm',
                              y=f'{metric}')

            
                    # разряжаем подписи по оси X
                    for ind, label in enumerate(ax2.get_xticklabels()):
                        if ind % 15 == 0:
                            label.set_visible(True)
                        else:
                            label.set_visible(False)
            
            
                    ax2.set(xlabel='time') # задаем имя оси Х
                    ax2.set(ylabel=f'{metric}') # задаем имя оси У
                    ax2.set_title(f'{metric} - {sigma} sigma range + rolling mean, 1 week before.') # задаем заголовок графика  
                    ax2.set(ylim=(0, None)) # задаем лимит для оси У
                    # формируем файловый объект
                    plot_object = io.BytesIO()
                    ax2.figure.savefig(plot_object)
                    plot_object.seek(0)
                    plot_object.name = f'{metric}.png'.format(metric)
                    plt.close()
                    bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)

                get_plot_7(df=df, sigma=sigma, metric=metric)
                print('Есть аномалии.')
                print('-'*60)       
            elif is_alert == 0:
                print('Аномалий не наблюдается.')
                print('-'*60)
        
        try:
            run_alerts(df=df, chat_id = group_chat_id, sigma = sigma, iqr=iqr)
        except Exception as e:
            print(e)        
        
        
    df = extract_data()        
    run_alerts(df=df, chat_id = group_chat_id, sigma = sigma, iqr=iqr)

edwan70_alerts = edwan70_alerts()

