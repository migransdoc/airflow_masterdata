import os
import requests
import datetime as dt
from datetime import datetime, timedelta
import sqlalchemy
import petl as etl
import psycopg2 as pg
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.sensors.sql import SqlSensor
from airflow.exceptions import AirflowSensorTimeout
from airflow.hooks.base import BaseHook


args = {
    'owner': 'balaev_ad',
    'start_date': datetime.strptime(Variable.get('start_date_ints'), '%Y-%m-%d %H:%M:%S'),
    'retries': 0,
    # 'retry_delay': timedelta(hours=1),
    'depends_on_past': False
    }

dag = DAG(dag_id='dag_int_01',
          default_args=args,
          schedule_interval=(lambda t: None if t == 'None' else timedelta(minutes=int(t)))(Variable.get('schedule_interval_ints'))
)

def python_find_load_id(**kwargs):
    """
    Нахождение load_id для текущего объекта, поток которого еще не был обработан
    """
    connection = BaseHook.get_connection('toir_postgres')
    connection_string_alchemy = 'postgresql://' + str(connection.login) + ':' + str(connection.password) + '@' + str(connection.host) + '/' + str(connection.schema)
    connection_string_pg = 'host=' + str(connection.host) + ' dbname=' + str(connection.schema) + ' user=' + str(connection.login) + ' password=' + str(connection.password)

    conn = pg.connect(connection_string_pg)

    query_result = etl.fromdb(conn, """select load_id as p_load_id from edw.etl_delta_log
                                where stg_status = 'N' and flow_id = 'INT_01' and target_lvl = 'ODSP'
                                order by crea_datetime asc limit 1;""")
    p_load_id = query_result['p_load_id'][0]
    task_instance = kwargs['task_instance']
    # Помещение load_id в xcom context для использования ее в следующих задачах
    task_instance.xcom_push(key='p_load_id', value=p_load_id)
    return p_load_id

def check_true_in_sensor(first_cell):
    """
    Функция для проверки сенсором наличия в дельта журнале новой необработанной строки
    """
    if first_cell >= 1:
        return True
    return False

# Функция, которая запускается, если сенсор затаймаутился (то есть условие успешного выполнения интегро потоков не было выполнено)
def _failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        raise AirflowFailException('Время выполнения сенсора закончилось, но в дельта журнале не появились нужные записи')

check_log_sensor = SqlSensor(conn_id='toir_postgres',
                             task_id='check_log_sensor',
                             dag=dag,
                             sql="""select count(*) from edw.etl_delta_log
                                where stg_status = 'N' and flow_id = 'INT_01' and target_lvl = 'ODSP';""",
                             poke_interval=int(Variable.get('poke_interval_sensor_ints')),  # интервал повторной проверки дельта журнала
                             timeout=int(Variable.get('timeout_sensor_ints')),  # если с момента запуска прошло n часов и сенсор не нашел новую строку, то выполнение дага прерывается
                             mode='reschedule',  # освобождение слота в airflow
                             success=check_true_in_sensor,
                             on_failure_callback=_failure_callback,  # если выполнение сенсора прервалось после таймаута
                             soft_fail=False  # Если выполнение сенсора прервалось, то все следующие задачи не будут запускаться и запускается on_failure_callback
                             )


find_load_id = PythonOperator(task_id='find_load_id',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=python_find_load_id,
                               dag=dag)

ods_to_odsp = PostgresOperator(task_id='ods_to_odsp',
                               postgres_conn_id='toir_postgres',
                               sql= 'sql_int_01/sql_int_01_odsp.sql',
                               dag=dag,
                               )

odsp_to_edw = PostgresOperator(task_id='odsp_to_edw',
                               postgres_conn_id='toir_postgres',
                               sql='sql_int_01/sql_int_01_edw.sql',
                               dag=dag,
                               )

delete_odsp = PostgresOperator(task_id='delete_odsp',
                               postgres_conn_id='toir_postgres',
                               sql='sql_int_01/sql_int_01_delete_odsp.sql',
                               dag=dag,
                               )

check_log_sensor >> find_load_id >> ods_to_odsp >> odsp_to_edw >> delete_odsp
