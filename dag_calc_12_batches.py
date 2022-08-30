import os
import requests
import datetime as dt
from datetime import datetime, timedelta
import sqlalchemy
import petl as etl
import psycopg2 as pg
from datetime import timedelta

from calc_12_functions.calc_01 import calc_01_python
from calc_12_functions.calc_02 import calc_02_python
from calc_12_functions.calc_04 import calc_04_python
from calc_12_functions.calc_05 import calc_05_python
from calc_12_functions.calc_07 import calc_07_python
from calc_12_functions.calc_08 import calc_08_python
from calc_12_functions.calc_09 import calc_09_python
from calc_12_functions.calc_10 import calc_10_python
from calc_12_functions.calc_18 import calc_18_python
from calc_12_functions.calc_16 import calc_16_python

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.sensors.sql import SqlSensor
from airflow.exceptions import AirflowSensorTimeout
from airflow.hooks.base import BaseHook


args = {
    'owner': 'balaev_ad',
    'start_date': datetime.strptime(Variable.get('start_date_calcs'), '%Y-%m-%d %H:%M:%S'),
    'retries': 0,   
    'retry_delay': timedelta(hours=1),
    'depends_on_past': False
    }

dag = DAG(dag_id='dag_calc_12_batches',
          default_args=args,
          schedule_interval=(lambda t: None if t == 'None' else timedelta(minutes=int(t)))(Variable.get('schedule_interval_calcs')))  # Интервал повторения запуска дага

def check_true_in_sensor(first_cell):
    """
    Функция для проверки сенсором наличия в журнале записей об успешном завершении всех интегро-потоков
    """
    if first_cell == 9 or first_cell == '9':
        return True
    return False

# Функция, которая запускается, если сенсор затаймаутился (то есть условие успешного выполнения интегро потоков не было выполнено)
def _failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        raise AirflowFailException('Время выполнения сенсора закончилось, но интегро-потоки не были завершены успешно, поэтому пересчет витрин не запускался')

# Сенсор проверяет условие: интегро-потоки INT_01..09 (все 9 штук) были завершены успешно в течение интервала с 18:00 прошлого дня до 08:00 текущего дня
check_log_sensor = SqlSensor(conn_id='toir_postgres',
                             task_id='check_log_sensor',
                             dag=dag,
                             sql="""select count(*)
                                    from edw.etl_delta_log
                                    where flow_id in ('INT_01', 'INT_02', 'INT_03', 'INT_04', 'INT_05', 'INT_06', 'INT_07', 'INT_08', 'INT_09')
                                        and target_lvl = 'EDW'
                                        and stg_status = 'S'
                                        and crea_datetime between (current_date - interval '1 day')::date + interval '18 hours' and current_date + interval '8 hours';""",
                             poke_interval=int(Variable.get('poke_interval_sensor_calc_12')),  # интервал повторной проверки дельта журнала
                             timeout=int(Variable.get('timeout_sensor_calc_12')),  # если с момента запуска прошло n часов и сенсор не нашел новую строку, то выполнение дага прерывается
                             mode='reschedule',  # освобождение слота в airflow
                             success=check_true_in_sensor,
                             on_failure_callback=_failure_callback,  # если выполнение сенсора прервалось после таймаута
                             soft_fail=False  # Если выполнение сенсора прервалось, то все следующие задачи не будут запускаться и запускается on_failure_callback
                             )

calc_divis = PostgresOperator(task_id='calc_divis',
                               #provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               postgres_conn_id='toir_postgres',
                               sql="""truncate im.divis;
                                        insert into im.divis
                                        select *
                                        from im.v_divis;""",
                               dag=dag)

# Задача по расчету витрины mt_stock
calc_01 = PythonOperator(task_id='calc_01',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=calc_01_python,
                               dag=dag)

# Задача по расчету витрины mt_movem_pt
calc_02 = PythonOperator(task_id='calc_02',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=calc_02_python,
                               dag=dag)

# Задача по расчету витрины mt_potr_snap_p
calc_04 = PythonOperator(task_id='calc_04',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=calc_04_python,
                               dag=dag)

# Задача по расчету плановых цен для mt_potr_snap_p
calc_05 = PythonOperator(task_id='calc_05',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=calc_05_python,
                               dag=dag)

# Задача по расчету витрины mt_pur_pt
calc_06 = PostgresOperator(task_id='calc_06',
                               postgres_conn_id='toir_postgres',
                               sql= 'sql_calc_06/sql_calc_06.sql',  # Вызывается SQL-код из файла
                               dag=dag,
                               )

# Задача по расчету витрины mt_pur_pt_snap_p
calc_07 = PythonOperator(task_id='calc_07',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=calc_07_python,
                               dag=dag)

# Задача по расчету витрины mt_req_pt
calc_08 = PythonOperator(task_id='calc_08',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=calc_08_python,
                               dag=dag)

# Задача по расчету витрины mt_req_snap_p
calc_09 = PythonOperator(task_id='calc_09',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=calc_09_python,
                               dag=dag)

# Задача по расчету витрины mt_potr
calc_10 = PythonOperator(task_id='calc_10',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=calc_10_python,
                               dag=dag)

# Задача по расчету витрины mt_special_stock
calc_18 = PythonOperator(task_id='calc_18',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=calc_18_python,
                               dag=dag)

# Задача по расчету витрины mt_stock_snap_p
calc_16 = PythonOperator(task_id='calc_16',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=calc_16_python,
                               dag=dag)

# Задача по расчету витрины im.mt_budget_stock
budget_stock = PostgresOperator(task_id='budget_stock',
                               postgres_conn_id='toir_postgres',
                               sql= """truncate im.mt_budget_stock;
                                    insert into im.mt_budget_stock(bs_werks, bs_o_divis, bs_r_divis, bs_year, bs_month, bs_month_year, bs_bwtar, bs_amt,
                                                                  bs_proizv, bs_or_divis_txt, bs_o_divis_txt, bs_r_divis_txt)
                                    select bs_werks,
                                          bs_o_divis,
                                          bs_r_divis,
                                          bs_year,
                                          bs_month,
                                          bs_month_year,
                                          bs_bwtar,
                                          bs_amt,
                                          bs_proizv,
                                          bs_or_divis_txt,
                                          bs_o_divis_txt,
                                          bs_r_divis_txt
                                    from cdm.v_budget_stock;""",  # Код полного очищения и вставки из cdm view
                               dag=dag,
                               )

# Задача по расчету витрины im.mt_budget_movem
budget_movem = PostgresOperator(task_id='budget_movem',
                               postgres_conn_id='toir_postgres',
                               sql= """truncate im.mt_budget_movem;
                                          insert into im.mt_budget_movem(bm_werks, bm_o_divis, bm_r_divis, bm_year, bm_month, bm_month_year, bm_bwtar, bm_section,
                                                                        bm_amt, bm_proizv, bm_or_divis_txt, bm_o_divis_txt, bm_r_divis_txt)
                                          select bm_werks,
                                                bm_o_divis,
                                                bm_r_divis,
                                                bm_year,
                                                bm_month,
                                                bm_month_year,
                                                bm_bwtar,
                                                bm_section,
                                                bm_amt,
                                                bm_proizv,
                                                bm_or_divis_txt,
                                                bm_o_divis_txt,
                                                bm_r_divis_txt
                                          from cdm.v_budget_movem;""",  # Код полного очищения и вставки из cdm view
                               dag=dag,
                               )


check_log_sensor >> calc_divis >> calc_10 >> calc_01 >> calc_16 >> calc_04 >> calc_05 >> calc_02 >> calc_06 >> calc_07 >> calc_08 >> calc_09 >> calc_18 >> budget_stock >> budget_movem
