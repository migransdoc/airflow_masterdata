import os
import requests
import datetime as dt
from datetime import datetime, timedelta
import sqlalchemy
import petl as etl
import psycopg2 as pg
from datetime import timedelta
import logging

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.sensors.sql import SqlSensor
from airflow.hooks.base import BaseHook

args = {
    'owner': 'balaev_ad',
    'start_date': datetime.strptime(Variable.get('start_date_calcs'), '%Y-%m-%d %H:%M:%S'),
    'retries': 0,
     'retry_delay': timedelta(hours=1),
    'depends_on_past': False
    }

dag = DAG(dag_id='dag_calc_17',
          default_args=args,
          schedule_interval=(lambda t: None if t == 'None' else timedelta(minutes=int(t)))(Variable.get('schedule_interval_calcs')))  # Интервал повторения запуска дага

def python_calc_17(**kwargs):
    """
    Нахождение load_id для функции, в отличие от интов, тут надо находить все возможные load_id, которые еще не были обработаны
    """
    connection = BaseHook.get_connection('toir_postgres')
    connection_string_alchemy = 'postgresql://' + str(connection.login) + ':' + str(connection.password) + '@' + str(connection.host) + '/' + str(connection.schema)
    connection_string_pg = 'host=' + str(connection.host) + ' dbname=' + str(connection.schema) + ' user=' + str(connection.login) + ' password=' + str(connection.password)

    conn = pg.connect(connection_string_pg)
    cursor = conn.cursor()
    engine = sqlalchemy.create_engine(connection_string_alchemy)

    query_result = etl.fromdb(conn, """select load_id as p_load_id from edw.etl_delta_log
                                where stg_status = 'N' and flow_id = 'CALC_17' and target_lvl = 'IM'
                                order by crea_datetime asc;""")
    p_load_id = query_result['p_load_id']
    p_load_id = list(p_load_id)  # Мы находим возможно несколько необработанных потокоов
    task_instance = kwargs['task_instance']
    # Помещение load_id в xcom context для использования ее в следующих задачах
    task_instance.xcom_push(key='p_load_id', value=p_load_id)

    for load_id in p_load_id:  # Итерируемся по потокам в цикле и отдельно для каждого потока запускаем make chain
      engine.execute(sqlalchemy.text("""update edw.etl_delta_log
set stg_status = 'I',
    row_count  = (select count(*)
                  from edw.potr_log
                  where load_id = '{load_id}')
where stg_status = 'N'
  and flow_id = 'CALC_17'
  and load_id = '{load_id}'
  and target_lvl = 'IM';
do
$$
    begin
        perform edw.make_chain_potr_log(p_load_id := '{load_id}');
    end
$$;
update edw.etl_delta_log
set stg_status = 'S',
last_delta_ts = now()
where flow_id = 'CALC_17'
  and target_lvl = 'IM'
  and load_id = '{load_id}';""".format(load_id=load_id)))  # Также в цикле обновляем значения дельта журнала
      
    cursor.close()
    conn.close()
    engine.dispose()
    return p_load_id

calc_17 = PythonOperator(task_id='calc_17',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=python_calc_17,
                               dag=dag)

calc_17
