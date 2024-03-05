# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from io import StringIO
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20240120',
                      'user':'user', 
                      'password':'password'
                     }

connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'user', 
                      'password':'password'
                     }
# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e.ananev',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 2, 7),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def edwan70_dag():

    @task()
    def extract_likes_views():
        query = '''
        SELECT toDate(time) as event_date,
               user_id,
               if(gender=1, 'male', 'female') as gender,
               age,
               os,
               countIf(user_id, action = 'view' ) as views,
               countIf(user_id, action = 'like' ) as likes

        FROM simulator_20240120.feed_actions
        WHERE toDate(time) = yesterday()
        GROUP BY user_id,
                 event_date,
                 gender,
                 age,
                 os '''
        df_likes_views = ph.read_clickhouse(query, connection=connection)
        return df_likes_views

    @task()
    def extract_sent_received():
        query = '''
        SELECT t1.event_date,
               t1.user_id,
               t1.gender,
               t1.age, 
               t1.os,
               t2.messages_received,
               t1.messages_sent,
               t2.users_received,
               t1.users_sent
        FROM
            (SELECT toDate(time) as event_date,
               user_id,
               uniq(receiver_id) as users_sent,
               count(receiver_id ) as messages_sent,
               if(gender=1, 'male', 'female') as gender,
               age,
               os
        FROM simulator_20240120.message_actions
        WHERE toDate(time) = yesterday()
        GROUP BY user_id,
                 event_date,
                 gender,
                 age,
                 os
        order by user_id desc) t1
        LEFT JOIN
            (SELECT toDate(time) as event_date,
                    receiver_id,
                    uniq(user_id) as users_received,
                    count(user_id) as messages_received
             FROM simulator_20240120.message_actions
             WHERE toDate(time) = yesterday()
             GROUP BY receiver_id,
                      event_date
             order by receiver_id desc) t2
        on t1.user_id == t2.receiver_id

        '''

        # Создание датафрейма
        df_sent_received = ph.read_clickhouse(query, connection=connection)
        return df_sent_received



    @task()
    def transform_join(df_sent_received, df_likes_views):
        df_likes_views_sent_received = df_sent_received.merge(df_likes_views, how='outer', 
                                                      left_on=['event_date','user_id', 'gender','age', 'os'], 
                                                      right_on=['event_date','user_id', 'gender','age', 'os']) \
                                                      .fillna(0).astype({'likes': np.int, 'views': np.int,
                                                         'messages_received': np.int, 'messages_sent': np.int,
                                                          'users_received': np.int,'users_sent': np.int})
        return df_likes_views_sent_received

    @task()
    def transform_os(df_likes_views_sent_received):
        df_os = df_likes_views_sent_received[['event_date', 'os', 'views', 'likes', 
                                              'messages_received','messages_sent', 
                                              'users_received', 'users_sent' ]] \
            .groupby(['event_date', 'os']) \
            .sum() \
            .reset_index()
        df_os['dimension']=df_os.columns[1]
        df_os.rename(columns = {'os': 'dimension_value'} , inplace=True)
        df_os.insert(
            1, 
            'dimension', 
            df_os.pop('dimension')
            )
        return df_os

    @task()
    def transform_gender(df_likes_views_sent_received):
        df_gender = df_likes_views_sent_received[['event_date', 'gender', 'views', 'likes', 
                                                  'messages_received','messages_sent', 
                                                  'users_received', 'users_sent' ]] \
            .groupby(['event_date', 'gender']) \
            .sum() \
            .reset_index()
        df_gender['dimension']=df_gender.columns[1]
        df_gender.rename(columns = {'gender': 'dimension_value'} , inplace=True)
        df_gender.insert(
            1, 
            'dimension', 
            df_gender.pop('dimension')
            )
        return df_gender

    @task()
    def transform_age(df_likes_views_sent_received):
        df_age = df_likes_views_sent_received[['event_date', 'age', 'views', 'likes', 
                                               'messages_received','messages_sent', 
                                               'users_received', 'users_sent' ]] \
            .groupby(['event_date', 'age']) \
            .sum() \
            .reset_index()
        df_age['dimension']=df_age.columns[1]
        df_age.rename(columns = {'age': 'dimension_value'} , inplace=True)
        df_age.insert(
            1, 
            'dimension', 
            df_age.pop('dimension')
            )
        return df_age


    @task()
    def transform_final_table(df_os, df_gender, df_age):
        df_final = pd.concat([df_os, df_gender, df_age], ignore_index=True, axis=0)

        return df_final


    @task()
    def load_log(df_os, df_gender, df_age):
        context = get_current_context()
        ds = context['ds']
        print(f'Metrics per os for {ds}')
        print(df_os.to_csv(index=False, sep='\t'))
        print(f'Metrics per gender for {ds}')
        print(df_gender.to_csv(index=False, sep='\t'))
        print(f'Metrics per age for {ds}')
        print(df_age.to_csv(index=False, sep='\t'))


    
    @task()
    def load_table(df_final):
        connection_test = {
        'database': 'test',
        'host': 'https://clickhouse.lab.karpov.courses',
        'user': 'student-rw',
        'password': '656e2b0c9c'
        }
        query_create_table = """
            CREATE TABLE IF NOT EXISTS test.res_table_edwan70 (
                event_date Date,
                dimension String,
                dimension_value String,
                views UInt32,
                likes UInt32,
                messages_received UInt32,
                messages_sent UInt32,
                users_received UInt32,
                users_sent UInt32
                ) 
                ENGINE MergeTree()
                order by event_date
            """
        ph.execute(query=query_create_table, connection=connection_test)
        ph.to_clickhouse(df_final, 'res_table_edwan70', index=False, connection=connection_test)

    #extract
    df_likes_views = extract_likes_views()
    df_sent_received = extract_sent_received()
    #join
    df_likes_views_sent_received = transform_join(df_sent_received, df_likes_views)
    #transform
    df_os = transform_os(df_likes_views_sent_received)
    df_gender = transform_gender(df_likes_views_sent_received)
    df_age = transform_age(df_likes_views_sent_received)
    df_final=transform_final_table(df_os, df_gender, df_age)
    #load
    load_log(df_os, df_gender, df_age)
    load_table(df_final)
    
edwan70_dag = edwan70_dag()
