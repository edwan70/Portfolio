# coding: utf-8
import pandahouse as ph
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import io
import telegram
import sys
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-ananev',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 2, 8),
}

schedule_interval = '0 11 * * *' # отчет приходит каждый день в 11 утра

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'password',
    'user': 'user',
    'database': 'simulator_20240120'
}


my_token = 'token'
bot = telegram.Bot(token=my_token)

chat_id = 00000000000
group_chat_id = -000000000000

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def edwan70_app_report():
   
    @task()
    # Загружаем данные ленты новостей за сегодняшний день, вчера и неделю назад
    def extract_today_yesterday_prev_week_feed():
        query_1 = """
    SELECT toDate(time) AS date, 
           uniq(user_id) as uniq_users,
           countIf(user_id, action = 'like') as likes,
           countIf(user_id, action = 'view') as views,
           ROUND(likes/views, 3) as ctr
    FROM simulator_20240120.feed_actions
    WHERE toDate(time) = today() or
    toDate(time) = yesterday() or
    toDate(time) = toDate(now()) - 7
    group by date
    order by date desc
     """

        report_today_feed = ph.read_clickhouse(query_1, connection=connection)
    
        #отправляем сообщение в чат
        uniq_users0 = report_today_feed['uniq_users'][0]
        views0 = report_today_feed['views'][0]
        likes0 = report_today_feed['likes'][0]
        ctr0 = report_today_feed['ctr'][0]
        uniq_users1 = report_today_feed['uniq_users'][1]
        views1 = report_today_feed['views'][1]
        likes1 = report_today_feed['likes'][1]
        ctr1 = report_today_feed['ctr'][1]
        uniq_users2 = report_today_feed['uniq_users'][2]
        views2 = report_today_feed['views'][2]
        likes2 = report_today_feed['likes'][2]
        ctr2 = report_today_feed['ctr'][2]
        date_to_show = report_today_feed['date'].max().strftime('%d-%m-%Y')
        msg = f'''
Отчет - оперативные данные\nпо ленте новостей\nза *{date_to_show}*:

*Количество уник. пользователей*
- Текущий день: {uniq_users0}
- Прошлый день: {uniq_users1}
- Неделю назад: {uniq_users2}

*Количество лайков*
- Текущий день: {likes0}
- Прошлый день: {likes1}
- Неделю назад: {likes2}

*Количество просмотров*
- Текущий день: {views0}
- Прошлый день: {views1}
- Неделю назад: {views2}

*CTR*
- Текущий день: {ctr0}
- Прошлый день: {ctr1}
- Неделю назад: {ctr2}'''
        bot.sendMessage(chat_id=group_chat_id, text=msg, parse_mode="Markdown")

    @task()
    # загружаем данные по метрикам из ленты новостей за сегодняшний день, вчера и неделю назад - интервал 10 минут
    def extract_feed():
        query_1 = '''
SELECT 
     t1.time AS time,
     t1.post_count as post_count,
     t2.post_count_prev_day as post_count_prev_day,
     t3.post_count_prev_week as post_count_prev_week,
     t1.dau as dau,
     t2.dau_prev_day as dau_prev_day,
     t3.dau_prev_week as dau_prev_week,
     t1.views as views,
     t2.views_prev_day as views_prev_day ,
     t3.views_prev_week as views_prev_week,
     t1.likes as likes,
     t2.likes_prev_day as likes_prev_day ,
     t3.likes_prev_week as likes_prev_week,
     ROUND(likes/views,3) AS ctr,
     ROUND(likes_prev_day/views_prev_day,3) AS ctr_prev_day,
     ROUND(likes_prev_week/views_prev_week,3) AS ctr_prev_week
FROM (SELECT  toStartOfTenMinutes(time) AS time,
              uniq(post_id) AS post_count,
              uniq(user_id) AS dau,
              countIf(user_id, action = 'view') AS views,
              countIf(user_id, action = 'like') AS likes,
              row_number() OVER(ORDER BY time) AS row_num
      FROM simulator_20240120.feed_actions
      WHERE toDate(time) >= toDate(now())
      GROUP BY time
      ORDER BY time) as t1 
      JOIN
      (SELECT
               toStartOfTenMinutes(time) AS time,
               uniq(post_id) AS post_count_prev_day,
               uniq(user_id) AS dau_prev_day,
               countIf(user_id, action = 'view') AS views_prev_day,
               countIf(user_id, action = 'like') AS likes_prev_day,
               row_number() OVER(ORDER BY time) AS row_num
       FROM simulator_20240120.feed_actions
       WHERE toDate(time) >= toDate(now()) - 1
       GROUP BY time
       ORDER BY time) as t2 
       ON t1.row_num = t2.row_num 
       JOIN 
       (SELECT
               toStartOfTenMinutes(time) AS time,
               uniq(post_id) AS post_count_prev_week,
               uniq(user_id) AS dau_prev_week,
               countIf(user_id, action = 'view') AS views_prev_week,
               countIf(user_id, action = 'like') AS likes_prev_week,
               row_number() OVER(ORDER BY time) AS row_num
        FROM simulator_20240120.feed_actions
        WHERE toDate(time) >= toDate(now()) - 7
        GROUP BY time
        ORDER BY time) as t3 
        ON t1.row_num = t3.row_num'''
        
        df_feed_metrics = ph.read_clickhouse(query_1, connection=connection)

        # отправляем графики ленты новостей в чат
        spisok_df_columns=df_feed_metrics.drop("time", axis=1).columns.values.tolist()
        spisok_title=['post_count', 'dau', 'views', 'likes', 'ctr']
        spisok_title_0 = ['сегодня', 'днем ранее', 'неделей ранее']
        spisok_title_1=['Количество постов - ', 'Количество пользователей - ', 
                        'Количество просмотров - ', 'Количество лайков - ', 'CTR - ']
        colors=['blue','green','coral', 'red', 'gold']
        def get_plot(df_feed_metrics=df_feed_metrics, chat_id=chat_id):
            plt.figure(figsize=(14, 5))
            ax1=sns.lineplot(data = df_feed_metrics, x = "time", y = spisok_df_columns[i], color = colors[0])\
            .set_title(f'Лента - {spisok_title_1[k]} {spisok_title[k]}:  {spisok_title_0[0]} - {colors[0]}, {spisok_title_0[1]} - {colors[1]}, {spisok_title_0[2]} - {colors[2]}', fontsize = 14)
            ax2=sns.lineplot(data = df_feed_metrics, x = "time", y = spisok_df_columns[i+1], color = colors[1])
            ax3=sns.lineplot(data = df_feed_metrics, x = "time", y = spisok_df_columns[i+2], color = colors[2])
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'Stats.png'
            plt.close()
            #bot.sendPhoto(chat_id=chat_id, photo=plot_object);
            bot.sendPhoto(chat_id=group_chat_id, photo=plot_object);
        k=0
        for i in range(0, len(spisok_df_columns)):
            if spisok_df_columns[i] ==  spisok_title[0]:
                get_plot()
                k+=1
            elif spisok_df_columns[i] ==  spisok_title[1]:
                get_plot()
                k+=1
            elif spisok_df_columns[i] ==  spisok_title[2]:
                get_plot()
                k+=1
            elif spisok_df_columns[i] ==  spisok_title[3]:
                get_plot()
                k+=1
            elif spisok_df_columns[i] ==  spisok_title[4]:
                get_plot()
                k=+1


    @task()
    # Загружаем данные по мессенджеру за сегодняшний день, вчера и неделю назад
    def extract_today_yesterday_prev_week_msg():
        query_1 = """
    SELECT toDate(time) AS date, 
           uniq(user_id) as uniq_users,
           count(user_id) as sent_msg
    FROM simulator_20240120.message_actions
    WHERE toDate(time) = today() or
    toDate(time) = yesterday() or
    toDate(time) = toDate(now()) - 7
    group by date
    order by date desc
     """

        report_today_msg = ph.read_clickhouse(query_1, connection=connection)
        uniq_users0 = report_today_msg['uniq_users'][0]
        sent_msg0 = report_today_msg['sent_msg'][0]
        uniq_users1 = report_today_msg['uniq_users'][1]
        sent_msg1 = report_today_msg['sent_msg'][1]
        uniq_users2 = report_today_msg['uniq_users'][2]
        sent_msg2 = report_today_msg['sent_msg'][2]
        date_to_show = report_today_msg['date'].max().strftime('%d-%m-%Y')
        msg = f'''
Отчет - оперативные данные\nпо месенджеру\nза *{date_to_show}*:

*Количество уник. пользователей*
- Текущий день: {uniq_users0}
- Прошлый день: {uniq_users1}
- Неделю назад: {uniq_users2}

*Количество сообщений*
- Текущий день: {sent_msg0}
- Прошлый день: {sent_msg1}
- Неделю назад: {sent_msg2}
'''
        bot.sendMessage(chat_id=group_chat_id, text=msg, parse_mode="Markdown")

    @task()
    # загружаем данные по метрикам из мессенджера за сегодняшний день, вчера и неделю назад - интервал 10 минут
    def extract_msg():
        query_2 = '''
SELECT
     t1.time as time,
     t1.dau as dau,
     dau_prev_day,
     dau_prev_week,
     send_msg,
     send_msg_prev_day,
     send_msg_prev_week
FROM(SELECT
        toStartOfTenMinutes(time) AS time,
        uniq(user_id) AS dau,
        count(user_id) AS send_msg,
        row_number() OVER(ORDER BY time) AS row_num
     FROM simulator_20240120.message_actions
     WHERE toDate(time) >= toDate(now())
     GROUP BY time
     ORDER BY time) AS t1 
JOIN
    (SELECT
          toStartOfTenMinutes(time) AS time,
          uniq(user_id) AS dau_prev_day,
          count(user_id) AS send_msg_prev_day,
          row_number() OVER(ORDER BY time) AS row_num
     FROM simulator_20240120.message_actions
     WHERE toDate(time) >= toDate(now()) - 1
     GROUP BY time
     ORDER BY time) AS t2 
ON t1.row_num = t2.row_num
JOIN
    (SELECT
          toStartOfTenMinutes(time) AS time,
          uniq(user_id) AS dau_prev_week,
          count(user_id) AS send_msg_prev_week,
          row_number() OVER(ORDER BY time) AS row_num
     FROM simulator_20240120.message_actions
     WHERE toDate(time) >= toDate(now()) - 7
     GROUP BY time
     ORDER BY time) as t3 
ON t1.row_num = t3.row_num'''
        
        df_msg_metrics = ph.read_clickhouse(query_2, connection=connection)
        

        # отправляем графики в чат
        def get_plot_dau_msg(df_msg_metrics=df_msg_metrics, chat_id=group_chat_id):
            #как менялось количество пользователей
            plt.figure(figsize=(14, 5))
            ax1 = sns.lineplot(x="time", y="dau", data=df_msg_metrics, color="red")
            ax1.set_title('Месенджер - количество пользователей-dau: сегодня-red, днем ранее-blue, неделей ранее-green', fontsize = 14)
            ax2 = sns.lineplot(x="time", y="dau_prev_day", data=df_msg_metrics, color="blue",)
            ax3 = sns.lineplot(x="time", y="dau_prev_week", data=df_msg_metrics, color="green",)
            plt.xlabel('Дата', fontsize = 14)
            plt.ylabel('Количество', fontsize = 14);
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'Stats.png'
            plt.close()
            #bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)
        
        def get_plot_msg_send_msg(df_msg_metrics=df_msg_metrics, chat_id=group_chat_id):
            #как менялось количество сообщений
            plt.figure(figsize=(14, 5))
            ax1 = sns.lineplot(x="time", y="send_msg", data=df_msg_metrics, color="red")
            ax1.set_title('Месенджер - количество сообщений - send_msg: сегодня - red, днем ранее - blue, неделей ранее - green', fontsize = 14)
            ax2 = sns.lineplot(x="time", y="send_msg_prev_day", data=df_msg_metrics, color="blue",)
            ax3 = sns.lineplot(x="time", y="send_msg_prev_week", data=df_msg_metrics, color="green",)
            plt.xlabel('Дата', fontsize = 14)
            plt.ylabel('Количество', fontsize = 14);
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'Stats.png'
            plt.close()
            #bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)
        
        get_plot_dau_msg(df_msg_metrics=df_msg_metrics, chat_id=group_chat_id)
        get_plot_msg_send_msg(df_msg_metrics=df_msg_metrics, chat_id=group_chat_id)
        
    @task()
    def extract_users():
        # Кол-во уникальных пользователей и ленты и месенджера и только ленты по ОС.
        query_1 = '''
SELECT date,
       os,
       sum(uniq_users_fm) as uniq_users_fm,
       sum(uniq_users_f) as uniq_users_f
FROM
(SELECT toDate(time) AS date,
       os AS os,
       source,
       sum(user_id) AS uniq_users_fm
FROM
  (with fa as
     (SELECT toDate(time) as time,
             uniq(user_id) as user_id,
             os,
             source
      FROM simulator_20240120.feed_actions
      group by time,
                os,
                source),
        ma as
     (SELECT distinct user_id
      FROM simulator_20240120.message_actions) 
      SELECT fa.time,
             fa.user_id,
             fa.os,
             fa.source
   FROM fa
   join ma using user_id
   order by time) AS temp_table
WHERE time = toDate(now()) or 
      time = toDate(now()) - 1 or
      time = toDate(now()) - 7
GROUP BY time,
         os,
         source
ORDER BY date DESC) t1
FULL OUTER JOIN
(SELECT toDate(time) as date,
       os AS os,
       source,
       sum(user_id) AS uniq_users_f
FROM
  (with fa as
     (SELECT toDate(time) as time,
             uniq(user_id) as user_id,
             os,
             source
      FROM simulator_20240120.feed_actions
      group by time,
                os,
                source),
        ma as
     (SELECT distinct user_id
      FROM simulator_20240120.message_actions) 
      SELECT fa.time,
             fa.user_id,
             fa.os,
             fa.source
      FROM fa
   left join ma using user_id
   where ma.user_id != fa.user_id
   order by time) AS virtual_table
WHERE time = toDate(now()) or 
      time = toDate(now()) - 1 or
      time = toDate(now()) - 7
GROUP BY date,
         os,
         source
ORDER BY date DESC) t2
ON t1.date = t2.date
GROUP BY date, os
ORDER by date desc
'''     
        df1 = ph.read_clickhouse(query_1, connection=connection)
        
        # Кол-во уникальных пользователей и ленты и месенджера и только ленты по источнику трафика.
        query_2 = '''
SELECT date,
       source,
       sum(uniq_users_fm) as uniq_users_fm,
       sum(uniq_users_f) as uniq_users_f
FROM
(SELECT toDate(time) AS date,
       os AS os,
       source,
       sum(user_id) AS uniq_users_fm
FROM
  (with fa as
     (SELECT toDate(time) as time,
             uniq(user_id) as user_id,
             os,
             source
      FROM simulator_20240120.feed_actions
      group by time,
                os,
                source),
        ma as
     (SELECT distinct user_id
      FROM simulator_20240120.message_actions) 
      SELECT fa.time,
             fa.user_id,
             fa.os,
             fa.source
   FROM fa
   join ma using user_id
   order by time) AS temp_table
WHERE time = toDate(now()) or 
      time = toDate(now()) - 1 or
      time = toDate(now()) - 7
GROUP BY time,
         os,
         source
ORDER BY date DESC) t1
FULL OUTER JOIN
(SELECT toDate(time) as date,
       os AS os,
       source,
       sum(user_id) AS uniq_users_f
FROM
  (with fa as
     (SELECT toDate(time) as time,
             uniq(user_id) as user_id,
             os,
             source
      FROM simulator_20240120.feed_actions
      group by time,
                os,
                source),
        ma as
     (SELECT distinct user_id
      FROM simulator_20240120.message_actions) 
      SELECT fa.time,
             fa.user_id,
             fa.os,
             fa.source
      FROM fa
   left join ma using user_id
   where ma.user_id != fa.user_id
   order by time) AS virtual_table
WHERE time = toDate(now()) or 
      time = toDate(now()) - 1 or
      time = toDate(now()) - 7
GROUP BY date,
         os,
         source
ORDER BY date DESC) t2
ON t1.date = t2.date
GROUP BY date, source
ORDER by date desc'''     
        
        df2 = ph.read_clickhouse(query_2, connection=connection)
        
        def send_msg_users(df1=df1, df2=df2, chat_id=group_chat_id):
            df_os = df1.groupby(['date']).agg({'uniq_users_f': sum, 'uniq_users_fm': sum}).reset_index()
            #df_source = df2.groupby(['date']).agg({'uniq_users_f': sum, 'uniq_users_fm': sum}).reset_index()
            uniq_users0_f = df_os['uniq_users_f'][2]
            uniq_users0_fm = df_os['uniq_users_fm'][2]
            uniq_users1_f = df_os['uniq_users_f'][1]
            uniq_users1_fm = df_os['uniq_users_fm'][1]
            uniq_users2_f = df_os['uniq_users_f'][0]
            uniq_users2_fm = df_os['uniq_users_fm'][0]
            date_to_show = df_os['date'].max().strftime('%d-%m-%Y')
            msg = f'''
Отчет - оперативные данные\nпо ленте новостей\nза *{date_to_show}*:

*Количество уник. пользователей* 
*Только лента* 
- Текущий день: {uniq_users0_f}
- Прошлый день: {uniq_users1_f}
- Неделю назад: {uniq_users2_f}

*Количество уник. пользователей* 
*Лента и месенджер*
- Текущий день: {uniq_users0_fm}
- Прошлый день: {uniq_users1_fm}
- Неделю назад: {uniq_users2_fm}
'''
            bot.sendMessage(chat_id=group_chat_id, text=msg, parse_mode="Markdown")
            
        send_msg_users(df1=df1, df2=df2, chat_id=group_chat_id)

        def send_png_users(df1=df1, df2=df2, chat_id=group_chat_id):
            fig, axes = plt.subplots(2, 2, figsize=(24, 24))

            fig.suptitle('Кол-во уникальных пользователей и ленты и месенджера и только ленты по ОС и источнику трафика.', fontsize=30)

            sns.barplot(ax = axes[0, 0], data = df1, x = 'date', y = 'uniq_users_fm', hue = 'os') \
            .set_xticklabels(np.unique(df1['date'].dt.date.values), rotation=30)
            axes[0, 0].set_title('Пользователи и ленты и месенджера по ОС.', fontsize=20)
            axes[0, 0].grid()
            
            sns.barplot(ax = axes[0, 1], data = df1, x = 'date', y = 'uniq_users_f', hue = 'os') \
            .set_xticklabels(np.unique(df1['date'].dt.date.values), rotation=30)
            axes[0, 1].set_title('Пользователи только ленты по ОС.' , fontsize=20)
            axes[0, 1].grid()

            sns.barplot(ax = axes[1, 0], data = df2, x = 'date', y = 'uniq_users_fm', hue = 'source') \
            .set_xticklabels(np.unique(df2['date'].dt.date.values), rotation=30)
            axes[1, 0].set_title('Пользователи и ленты и месенджера по каналу.', fontsize=20)
            axes[1, 0].grid()
            
            sns.barplot(ax = axes[1, 1], data = df2, x = 'date', y = 'uniq_users_f', hue = 'source') \
            .set_xticklabels(np.unique(df2['date'].dt.date.values), rotation=30)
            axes[1, 1].set_title('Пользователи только ленты по каналу.', fontsize=20)
            axes[1, 1].grid()

            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'Stats.png'
            plt.close()
    
            bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)
        
        send_png_users(df1=df1, df2=df2, chat_id=group_chat_id)
    
    report_today_feed = extract_today_yesterday_prev_week_feed()
    df_feed_metrics = extract_feed()
    report_today_msg = extract_today_yesterday_prev_week_msg()
    df_msg_metrics = extract_msg()
    extract_users = extract_users()
    
    report_today_feed >> df_feed_metrics >> report_today_msg >> df_msg_metrics >> extract_users

edwan70_app_report = edwan70_app_report()






                
