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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.sensors.sql import SqlSensor
from airflow.exceptions import AirflowSensorTimeout
from airflow.hooks.base import BaseHook

args = {
    'owner': 'balaev_ad',
    'start_date': datetime.strptime(Variable.get('start_date_super_dag'), '%Y-%m-%d %H:%M:%S'),
    'retries': 0,
    'retry_delay': timedelta(hours=1),
    #'depends_on_past': False
    }

dag = DAG(dag_id='dag_all_calc_only',
          default_args=args,
          schedule_interval=(lambda t: None if t == 'None' else timedelta(minutes=int(t)))(Variable.get('schedule_interval_super_dag')),
          catchup=False
        )

trigger_dag_calc_12 = TriggerDagRunOperator(task_id='trigger_dag_calc_12',
                                            trigger_dag_id='dag_calc_12_batches',
                                            wait_for_completion=True,  # не позволяет след таскам запускаться, пока не отработает полностью этот
                                            reset_dag_run=True,  # позволяет делать реран дага в ту же дату
                                            execution_date='{{ ds }}',  # назначсет дочернему дагу время исполнения этого
                                            poke_interval=5,  # каждые 5 сек чекает статус исполнения дага
                                            failed_states=None,
                                            dag=dag
                                            )

trigger_dag_calc_13 = TriggerDagRunOperator(task_id='trigger_dag_calc_13',
                                            trigger_dag_id='dag_calc_13',
                                            wait_for_completion=True,  # не позволяет след таскам запускаться, пока не отработает полностью этот
                                            reset_dag_run=True,  # позволяет делать реран дага в ту же дату
                                            execution_date='{{ ds }}',  # назначсет дочернему дагу время исполнения этого
                                            poke_interval=15,  # каждые n сек чекает статус исполнения дага
                                            failed_states=None,
                                            dag=dag
                                            )

trigger_dag_calc_15 = TriggerDagRunOperator(task_id='trigger_dag_calc_15',
                                            trigger_dag_id='dag_calc_15',
                                            wait_for_completion=True,  # не позволяет след таскам запускаться, пока не отработает полностью этот
                                            reset_dag_run=True,  # позволяет делать реран дага в ту же дату
                                            execution_date='{{ ds }}',  # назначсет дочернему дагу время исполнения этого
                                            poke_interval=15,  # каждые n сек чекает статус исполнения дага
                                            #allowed_states=['success', 'failed'],  # даг переместится к след задаче в любом случае, так как кальки 13, 15 и 17 независимы
                                            failed_states=None,
                                            #trigger_rule='all_done',  # таск запустится в любом случае, так как кальк 15, кальк 13 и кальк 17 независимы
                                            dag=dag
                                            )

trigger_dag_calc_17 = TriggerDagRunOperator(task_id='trigger_dag_calc_17',
                                            trigger_dag_id='dag_calc_17',
                                            wait_for_completion=True,  # не позволяет след таскам запускаться, пока не отработает полностью этот
                                            reset_dag_run=True,  # позволяет делать реран дага в ту же дату
                                            execution_date='{{ ds }}',  # назначсет дочернему дагу время исполнения этого
                                            poke_interval=15,  # каждые n сек чекает статус исполнения дага
                                            failed_states=None,
                                            dag=dag
                                            )

trigger_dag_calc_12 >> trigger_dag_calc_13 >> trigger_dag_calc_15 >> trigger_dag_calc_17
