# coding: utf-8
import pandahouse as ph
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import io
import telegram

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

schedule_interval = '0 11 * * *' # отчет приходит каждый день в 11 утра

my_token = 'token'
bot = telegram.Bot(token=my_token)

chat_id = 00000000000
group_chat_id = -00000000000

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def edwan70_feed_report_1():
   
    @task()
    # Загружаем данные за прошлый день
    def extract_yesterday():
        query_1 = """
        SELECT toDate(time) as date, 
               uniq(user_id) as dau, 
               countIf(user_id, action = 'like') as likes,
               countIf(user_id, action = 'view') as views, 
               likes/views as CTR
        FROM simulator_20240120.feed_actions
        WHERE toDate(time) = yesterday()
        group by date
        """

        report_last_day = ph.read_clickhouse(query_1, connection=connection)
        return report_last_day
    
    @task()
    # Загружаем данные за прошлую неделю
    def extract_last_week():
        
        query_2 = """
        SELECT toDate(time) as date, 
               uniq(user_id) as dau, 
               countIf(user_id, action = 'like') as likes,
               countIf(user_id, action = 'view') as views, 
               likes/views as CTR
        FROM simulator_20240120.feed_actions
        WHERE toDate(time) > today()-8 AND toDate(time) < today()
        GROUP BY date
        """

        report_week = ph.read_clickhouse(query_2, connection=connection)
        return report_week

    @task()
    def send_message_yesterday(report_last_day, chat_id):
        dau = report_last_day['dau'].sum()
        views = report_last_day['views'].sum()
        likes = report_last_day['likes'].sum()
        ctr = report_last_day['CTR'].sum()
        date_to_show = report_last_day['date'].max().strftime('%d-%m-%Y')

        msg = '-' * 40 + '\n\n' + \
        f'Статистика ленты новостей\n за вчера {date_to_show}:\ndau: {dau}\nПросмотры: {views}\nЛайки: {likes}\nCTR: {ctr:.2f}\n' + \
        '-' * 40 + '\n'

        #bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendMessage(chat_id=group_chat_id, text=msg)
        
        
    @task()
    def send_png_week(report_week, chat_id):
        fig, axes = plt.subplots(4, 1, figsize=(15, 20))

        fig.suptitle('Динамика за последние 7 дней', fontsize=30)

        sns.lineplot(ax = axes[0], data = report_week, x = 'date', y = 'dau')
        axes[0].set_title('dau')
        axes[0].grid()

        sns.lineplot(ax = axes[1], data = report_week, x = 'date', y = 'CTR')
        axes[1].set_title('CTR')
        axes[1].grid()

        sns.lineplot(ax = axes[2], data = report_week, x = 'date', y = 'views')
        axes[2].set_title('Просмотры')
        axes[2].grid()

        sns.lineplot(ax = axes[3], data = report_week, x = 'date', y = 'likes')
        axes[3].set_title('Лайки')
        axes[3].grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Stats.png'
        plt.close()

        #bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)
        
    report_last_day = extract_yesterday()
    report_week = extract_last_week()
    send_message_yesterday(report_last_day, chat_id)
    send_png_week(report_week, chat_id)
        

edwan70_feed_report_1 = edwan70_feed_report_1()
