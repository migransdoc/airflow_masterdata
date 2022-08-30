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
from airflow.hooks.base import BaseHook


args = {
    'owner': 'balaev_ad',
    'start_date': datetime.strptime(Variable.get('start_date_ints'), '%Y-%m-%d %H:%M:%S'),
    'retries': 0,
    # 'retry_delay': timedelta(hours=1),
    'depends_on_past': False
    }

dag = DAG(dag_id='dag_int_10',
          default_args=args,
           schedule_interval=timedelta(minutes=int(Variable.get('schedule_interval_int_10_11'))))  # Интервал повторения запуска дага
           
def python_int_10(**kwargs):
    odsp = """update edw.etl_delta_log
set stg_status = 'I',
    row_count  = (select count(*)
                  from ods.toro_snap_b
                  where load_id = '{load_id}')
where stg_status = 'N'
  and flow_id = 'INT_10'
  and load_id = '{load_id}'
  and target_lvl = 'ODSP';

insert into odsp.toro_snap_b (source_snap_id, source_author_name, source_crea_date_time, source_snap_type, excep,
                              werks, start_month, tech_divis, tech_divis_text, work_divis, work_divis_text, pspel,
                              tplnr, pltxt, kostl, kostv, vaplz, ekgrp, srvpos, asktx, arbpl, arbpl_ktext, auart,
                              ingpr, ktext, aufnr, user4, cat12_3, cat11_3, cat11_1, cat11_2, cat12_1, cat12_2,
                              cat13_3, cat13_1, cat13_2, cat21, cat22, cat32, cat42, cat41, arbei, arbpl_werks,
                              steus, kstar, ilart, zpost1, load_id, load_dttm, extraction_query_id, source_cd)
select source_snap_id,
       source_author_name,
       source_crea_date_time,
       source_snap_type,
       excep,
       werks,
       start_month,
       tech_divis,
       tech_divis_text,
       work_divis,
       work_divis_text,
       pspel,
       tplnr,
       pltxt,
       kostl,
       kostv,
       vaplz,
       ekgrp,
       srvpos,
       asktx,
       arbpl,
       arbpl_ktext,
       auart,
       ingpr,
       ktext,
       aufnr,
       user4,
       cat12_3,
       cat11_3,
       cat11_1,
       cat11_2,
       cat12_1,
       cat12_2,
       cat13_3,
       cat13_1,
       cat13_2,
       cat21,
       cat22,
       cat32,
       cat42,
       cat41,
       arbei,
       arbpl_werks,
       steus,
       kstar,
       ilart,
       zpost1,
       load_id,
       load_dttm,
       extraction_query_id,
       source_snap_id || ' ' || source_author_name || ' ' || source_crea_date_time::varchar || ' ' ||
       source_snap_type
from ods.toro_snap_b
where load_id = '{load_id}';

update edw.etl_delta_log
set stg_status = 'S'
where flow_id = 'INT_10'
  and target_lvl = 'ODSP'
  and load_id = '{load_id}';
"""

    edw = """insert into edw.etl_delta_log (flow_id,
                               crea_datetime,
                               target_lvl,
                               row_count,
                               load_id,
                               stg_status)
values ('INT_10', now(), 'EDW',
        (select count(*)
         from odsp.toro_snap_b
         where load_id = '{load_id}'),
        '{load_id}',
        'I');

delete
from edw.toro_snap_b
where source_snap_id in (select source_snap_id
                         from odsp.toro_snap_b
                         where load_id = '{load_id}'
                         group by 1);

insert into edw.toro_snap_b (source_snap_id, source_author_name, source_crea_date_time, source_snap_type, excep,
                             werks, start_month, tech_divis, tech_divis_text, work_divis, work_divis_text, pspel,
                             tplnr, pltxt, kostl, kostv, vaplz, ekgrp, srvpos, asktx, arbpl, arbpl_ktext, auart,
                             ingpr, ktext, aufnr, user4, cat12_3, cat11_3, cat11_1, cat11_2, cat12_1, cat12_2,
                             cat13_3, cat13_1, cat13_2, cat21, cat22, cat32, cat42, cat41, arbei, arbpl_werks,
                             steus, kstar, ilart, zpost1, load_id, dataflow_id, source_cd, load_dttm)
select source_snap_id,
       source_author_name,
       source_crea_date_time,
       source_snap_type,
       excep,
       werks,
       start_month,
       tech_divis,
       tech_divis_text,
       work_divis,
       work_divis_text,
       pspel,
       tplnr,
       pltxt,
       kostl,
       kostv,
       vaplz,
       ekgrp,
       srvpos,
       asktx,
       arbpl,
       arbpl_ktext,
       auart,
       ingpr,
       ktext,
       aufnr,
       user4,
       cat12_3,
       cat11_3,
       cat11_1,
       cat11_2,
       cat12_1,
       cat12_2,
       cat13_3,
       cat13_1,
       cat13_2,
       cat21,
       cat22,
       cat32,
       cat42,
       cat41,
       arbei,
       arbpl_werks,
       steus,
       kstar,
       ilart,
       zpost1,
       load_id,
       'INT_10',
       source_cd,
       now()
from odsp.toro_snap_b
where load_id = '{load_id}';

-- обновление статуса на success
update edw.etl_delta_log
set stg_status = 'S'
where flow_id = 'INT_10'
  and target_lvl = 'EDW'
  and load_id = '{load_id}';
"""

    delete_odsp = """delete
from odsp.toro_snap_b
where load_id = '{load_id}';
"""

    """
    Нахождение load_id для функции, в отличие от остальных интов, тут надо находить все возможные load_id, которые еще не были обработаны
    """
    connection = BaseHook.get_connection('toir_postgres')
    connection_string_alchemy = 'postgresql://' + str(connection.login) + ':' + str(connection.password) + '@' + str(connection.host) + '/' + str(connection.schema)
    connection_string_pg = 'host=' + str(connection.host) + ' dbname=' + str(connection.schema) + ' user=' + str(connection.login) + ' password=' + str(connection.password)

    conn = pg.connect(connection_string_pg)
    cursor = conn.cursor()
    engine = sqlalchemy.create_engine(connection_string_alchemy)

    query_result = etl.fromdb(conn, """select load_id as p_load_id from edw.etl_delta_log
                                where stg_status = 'N' and flow_id = 'INT_10' and target_lvl = 'ODSP';""")
    p_load_id = query_result['p_load_id']
    p_load_id = list(p_load_id)  # Мы находим возможно несколько необработанных потокоов

    for load_id in p_load_id:
        engine.execute(sqlalchemy.text(odsp.format(load_id=load_id)))
        engine.execute(sqlalchemy.text(edw.format(load_id=load_id)))
        engine.execute(sqlalchemy.text(delete_odsp.format(load_id=load_id)))

    cursor.close()
    conn.close()
    engine.dispose()

int_10 = PythonOperator(task_id='int_10',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=python_int_10,
                               dag=dag)

int_10
