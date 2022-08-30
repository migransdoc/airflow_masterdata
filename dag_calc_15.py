import os
import requests
import datetime as dt
from datetime import datetime, timedelta
import sqlalchemy
import petl as etl
import psycopg2 as pg
import time
# import dateutil
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import sessionmaker, scoped_session

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
    'retry_delay': timedelta(minutes=30),
    'depends_on_past': False
    }

dag = DAG(dag_id='dag_calc_15',
          default_args=args,
          schedule_interval=(lambda t: None if t == 'None' else timedelta(minutes=int(t)))(Variable.get('schedule_interval_calcs')))  # Интервал повторения запуска дага
          #schedule_interval='0 5 5 * *')  # Интервал повторения запуска дага раз в месяц 0 5 5 * *

def python_sample_var(**kwargs):

    connection = BaseHook.get_connection('toir_postgres')
    connection_string_alchemy = 'postgresql://' + str(connection.login) + ':' + str(connection.password) + '@' + str(connection.host) + '/' + str(connection.schema)
    connection_string_pg = 'host=' + str(connection.host) + ' dbname=' + str(connection.schema) + ' user=' + str(connection.login) + ' password=' + str(connection.password)

    #conn = pg.connect(connection_string_pg)
    #cursor = conn.cursor()
    #engine = sqlalchemy.create_engine(connection_string_alchemy)

    MONTHS_SUM = 12
    curr_date = dt.date.today().replace(day=1)
    # prev_month = curr_date - relativedelta(months=1)  # Это sca_date в коде на скл, первый расчетный месяц
    for i in range(0, MONTHS_SUM):
        curr_month = curr_date - relativedelta(months=i)

        # Шаг 1 - очищение первичной таблицы
            
        engine = sqlalchemy.create_engine(connection_string_alchemy, echo = True)
        Session = scoped_session(sessionmaker(bind=engine))
        s = Session()
        s.execute(sqlalchemy.text("""
ANALYZE im.mt_stock_categ_calc;
ANALYZE im.mt_stock_categ_calc_enrich;
DELETE FROM im.mt_stock_categ_calc
WHERE sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month';
ANALYZE im.mt_stock_categ_calc;
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 2 - очищение обогащенной таблицы        
        s.execute(sqlalchemy.text("""
DELETE FROM im.mt_stock_categ_calc_enrich
WHERE sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month';
ANALYZE im.mt_stock_categ_calc_enrich;
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 3 - Insert Запас Итого
        s.execute(sqlalchemy.text("""
INSERT INTO im.mt_stock_categ_calc(sca_werks, sca_o_divis, sca_r_divis, sca_matnr, sca_date, sca_qty, sca_amt, sca_kat)
select st_werks,
	coalesce(st_o_divis,''),
    coalesce(st_r_divis,''),
    st_matnr,
    st_date,
    coalesce(sum(st_qty), 0),
    coalesce(sum(st_amt), 0),
    4::smallint
FROM im.mt_stock
WHERE st_qty > 0 and st_amt > 0 and st_date >= '{sca_date}'::date and st_date < '{sca_date}'::date + interval '1 month'
group by 1, 2, 3, 4, 5;
ANALYZE im.mt_stock_categ_calc;
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 4 - Insert Страхового запаса (разметка)
        s.execute(sqlalchemy.text("""	
INSERT INTO im.mt_stock_categ_calc(sca_werks, sca_o_divis, sca_r_divis, sca_matnr, sca_date, sca_qty, sca_kat)
    select t3.werks,
    t3.o_divis, 
    t3.r_divis, 
    t3.matnr, 
    t3.date, 
    case when t4.qty > t3.sca_qty then t3.sca_qty else t4.qty end,
    1::smallint
from
(
    select sca_werks as werks,
		sca_date as date,
		sca_o_divis as o_divis,
		sca_r_divis as r_divis,
		sca_matnr   as matnr,
		sca_qty
    from im.mt_stock_categ_calc
    where sca_kat = 4
    and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month'
) t3
join
(
    select t5.werks,
		t5.date, 
		t5.o_divis, 
		t5.r_divis, 
		t5.matnr, 
		coalesce(t5.qty,0) as qty
    from
    (
		select t2.werks,
			t1.date,
			t2.o_divis,
			t2.r_divis,
			t1.matnr,
			sum(qty) as qty
		from
		(
			select rs_ppm_area,
			date(date_trunc('month', rs_load_date)) as date, 
			rs_matnr as matnr,
			rs_qty_avlb as qty
			from edw.reqstock
			where rs_tech_data_type = 1 and rs_ppm_elem = '1S' and rs_prof_load in (1,2) 
			and date(date_trunc('month', rs_load_date)) >= date(date_trunc('month', current_date))
			and date(date_trunc('month', rs_load_date)) < date(date_trunc('month', current_date)) + interval '1 month'
			union
			select rs_ppm_area,
			date(date_trunc('month', rs_load_date)) as date, 
			rs_matnr as matnr,
			rs_qty_avlb as qty
			from edw.reqstock
			where rs_tech_data_type = 3 and rs_ppm_elem = '1S' and rs_prof_load in (1,2) 
			and date(date_trunc('month', rs_load_date)) < date(date_trunc('month', current_date))
		) t1
		join
		(
			select wm_oblast_ppm, 
				max(wm_o_divis) as o_divis, 
				max(wm_r_divis) as r_divis, 
				max(wm_werks) as werks
			from edw.dim_wh_map
			where wm_oblast_ppm <> ''
			group by 1
		) t2
		on t1.rs_ppm_area = t2.wm_oblast_ppm
		group by 1,2,3,4,5
    ) t5
    where t5.date >= '{sca_date}'::date and t5.date < '{sca_date}'::date + interval '1 month'
) t4
on t3.werks = t4.werks and t3.date = t4.date and t3.o_divis = t4.o_divis and t3.r_divis = t4.r_divis and t3.matnr = t4.matnr;
ANALYZE im.mt_stock_categ_calc;
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 5 - Insert Долголежащего запаса
        s.execute(sqlalchemy.text("""
INSERT INTO im.mt_stock_categ_calc(sca_werks, sca_o_divis, sca_r_divis, sca_matnr, sca_date, sca_qty, sca_kat)
select werks,
	o_divis,
    r_divis,
    matnr,
    date,
    min_itogo_qty as dol_qty,
    2::smallint
from (
            select t7.st_werks as werks,
				t7.date,
                t7.st_o_divis as o_divis,
                t7.st_r_divis as r_divis,
                t7.st_matnr as matnr,
                min(t7.st_qty - coalesce(t8.str_qty, 0))
                over (partition by st_o_divis, st_r_divis, st_matnr) as min_itogo_qty
			from 
			(

                select t4.st_werks, t4.date, t4.st_o_divis, t4.st_r_divis, t4.st_matnr, coalesce(t5.st_qty, 0) as st_qty
                from (
                        select *
                        from (
                                select date
                                from edw.tech_unique_date
                                where date > '{sca_date}'::date - interval '12 month'
                                and date <= '{sca_date}'::date
                                group by 1
                             ) t1
                             cross join
                             (
                                select sca_werks as st_werks,
									sca_o_divis as st_o_divis,
									sca_r_divis as st_r_divis,
									sca_matnr as st_matnr
                                from im.mt_stock_categ_calc
                                where sca_kat = 4 and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month'
                             ) t2
                    ) t4
                    left join
                    (
                        select st_werks,
							st_date,
							coalesce(st_o_divis,'') as st_o_divis,
							coalesce(st_r_divis,'') as st_r_divis,
							st_matnr,
							sum(st_qty) as st_qty
                        from im.mt_stock
                        where st_date <= '{sca_date}'::date and st_date > '{sca_date}'::date - interval '12 month'
                        group by 1,2,3,4,5
                    ) t5
                    on t4.st_werks = t5.st_werks and t4.date = t5.st_date and t4.st_o_divis = t5.st_o_divis and
                    t4.st_r_divis = t5.st_r_divis and t4.st_matnr = t5.st_matnr
			) t7
            left join
            (
                select t10.werks,
					t10.date, 
					t10.o_divis, 
					t10.r_divis, 
					t10.matnr, 
					t10.qty as str_qty
                from
                (
					select t2.werks,
						t1.date,
						t2.o_divis,
						t2.r_divis,
						t1.matnr,
						sum(qty) as qty
					from
					(
						select rs_ppm_area,
						date(date_trunc('month', rs_load_date)) as date, 
						rs_matnr as matnr,
						rs_qty_avlb as qty
						from edw.reqstock
						where rs_tech_data_type = 1 and rs_ppm_elem = '1S' and rs_prof_load in (1,2) 
						and date(date_trunc('month', rs_load_date)) >= date(date_trunc('month', current_date))
						and date(date_trunc('month', rs_load_date)) < date(date_trunc('month', current_date)) + interval '1 month'
						union
						select rs_ppm_area,
						date(date_trunc('month', rs_load_date)) as date, 
						rs_matnr as matnr,
						rs_qty_avlb as qty
						from edw.reqstock
						where rs_tech_data_type = 3 and rs_ppm_elem = '1S' and rs_prof_load in (1,2) 
						and date(date_trunc('month', rs_load_date)) < date(date_trunc('month', current_date))
					) t1
					join
					(
						select wm_oblast_ppm, 
							max(wm_o_divis) as o_divis, 
							max(wm_r_divis) as r_divis, 
							max(wm_werks) as werks
						from edw.dim_wh_map
						where wm_oblast_ppm <> ''
						group by 1
					) t2
					on t1.rs_ppm_area = t2.wm_oblast_ppm
					group by 1,2,3,4,5
                ) t10
                where t10.date <= '{sca_date}'::date and t10.date > '{sca_date}'::date - interval '12 month'
            ) t8
            on t7.st_werks = t8.werks and t7.date = t8.date and t7.st_o_divis = t8.o_divis and
            t7.st_r_divis = t8.r_divis and t7.st_matnr = t8.matnr
    ) t9
where date >= '{sca_date}'::date and date < '{sca_date}'::date + interval '1 month' and min_itogo_qty > 0;
ANALYZE im.mt_stock_categ_calc;
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 6 - Insert Быстрооборачиваемого запаса
        s.execute(sqlalchemy.text("""
INSERT INTO im.mt_stock_categ_calc(sca_werks, sca_o_divis, sca_r_divis, sca_matnr, sca_date, sca_qty, sca_kat)
select t1.sca_werks,
	t1.sca_o_divis,
    t1.sca_r_divis,
    t1.sca_matnr,
    t1.sca_date,
    t1.qty_itog - coalesce(t2.qty_str,0) - coalesce(t3.qty_dol,0) as qty_bis,
    3::smallint
from (
        select sca_werks,
			sca_date,
			sca_o_divis,
			sca_r_divis,
			sca_matnr,
			sca_qty as qty_itog
        from im.mt_stock_categ_calc
        where sca_kat = 4
        and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month'
    ) t1
        left join
    (
        select sca_werks,
			sca_date,
			sca_o_divis,
			sca_r_divis,
			sca_matnr,
			sca_qty as qty_str
        from im.mt_stock_categ_calc
        where sca_kat = 1
        and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month'
    ) t2
    on t1.sca_werks = t2.sca_werks and t1.sca_date = t2.sca_date and t1.sca_o_divis = t2.sca_o_divis and t1.sca_r_divis = t2.sca_r_divis and
        t1.sca_matnr = t2.sca_matnr
        left join
    (
        select sca_werks,
			sca_date,
			sca_o_divis,
			sca_r_divis,
			sca_matnr,
			sca_qty as qty_dol
        from im.mt_stock_categ_calc
        where sca_kat = 2
        and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month'
    ) t3
    on t1.sca_werks = t3.sca_werks and t1.sca_date = t3.sca_date and t1.sca_o_divis = t3.sca_o_divis and t1.sca_r_divis = t3.sca_r_divis and t1.sca_matnr = t3.sca_matnr;
ANALYZE im.mt_stock_categ_calc;
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 7 - Update поля 'стоимость' (sca_amt) 
        s.execute(sqlalchemy.text("""
UPDATE im.mt_stock_categ_calc t1
SET sca_amt = t2.sca_price * sca_qty
FROM
(
	SELECT sca_werks, sca_date, sca_o_divis, sca_r_divis, sca_matnr, (sca_amt / sca_qty) as sca_price
	FROM im.mt_stock_categ_calc
	WHERE sca_kat = 4 and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month'
) t2
WHERE sca_kat in (1,2,3) and t1.sca_werks = t2.sca_werks and t1.sca_date = t2.sca_date and t1.sca_o_divis = t2.sca_o_divis 
and t1.sca_r_divis = t2.sca_r_divis and t1.sca_matnr = t2.sca_matnr and 
t1.sca_date >= '{sca_date}'::date and t1.sca_date < '{sca_date}'::date + interval '1 month';
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 8 - Insert данных из первичной таблицы в обогащённую
        s.execute(sqlalchemy.text("""
INSERT INTO im.mt_stock_categ_calc_enrich(sca_werks, sca_o_divis, sca_r_divis, sca_matnr, sca_date, sca_qty, sca_amt, sca_kat)
SELECT sca_werks,
	sca_o_divis,
    sca_r_divis,
    sca_matnr,
    sca_date,
    sca_qty,
    sca_amt,
    sca_kat
from im.mt_stock_categ_calc
where sca_kat in (1,2,3) and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month';
ANALYZE im.mt_stock_categ_calc_enrich;
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 7 - Update полей year, month
        s.execute(sqlalchemy.text("""
UPDATE im.mt_stock_categ_calc_enrich
SET sca_year  = date_part('year', sca_date),
    sca_month = date_part('month', sca_date)
WHERE sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month';
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 8 - Update поля sca_proizv
        s.execute(sqlalchemy.text("""
UPDATE im.mt_stock_categ_calc_enrich t1
SET sca_proizv = t2.wm_proizv
FROM edw.dim_wh_map t2
WHERE t1.sca_werks = t2.wm_werks 
and t1.sca_o_divis = t2.wm_o_divis
and t1.sca_r_divis = t2.wm_r_divis
and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month';
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 9 - Update полей sca_matkl, sca_matnr_txt, sca_ekgrp
        s.execute(sqlalchemy.text("""
UPDATE im.mt_stock_categ_calc_enrich t1 
SET sca_matkl = t2.mt_matkl,
	sca_matnr_txt = t2.mt_cc_idtext,
	sca_ekgrp = t2.mt_ekgrp
FROM edw.dim_mat_txt t2 WHERE t1.sca_matnr = t2.mt_matnr and t1.sca_werks = t2.mt_werks
and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month';
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 9 - Update поля sca_bwtar
        s.execute(sqlalchemy.text("""
UPDATE im.mt_stock_categ_calc_enrich t1
SET sca_bwtar = st_bwtar
FROM 
(SELECT st_werks, st_o_divis, st_r_divis, st_matnr, max(st_bwtar) st_bwtar from im.mt_stock WHERE st_date = '{sca_date}'::date GROUP BY 1,2,3,4) t2
WHERE t1.sca_werks = t2.st_werks
and t1.sca_o_divis = t2.st_o_divis 
and t1.sca_r_divis = t2.st_r_divis and t1.sca_matnr = t2.st_matnr
and sca_date = '{sca_date}'::date;
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 10 - Update поля sca_matkl_txt
        s.execute(sqlalchemy.text("""
UPDATE im.mt_stock_categ_calc_enrich t1
SET sca_matkl_txt = mtg_cc_idtext
FROM edw.dim_mat_gr_txt t2
WHERE t1.sca_matkl = t2.mtg_matkl
and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month';
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 11 - Update поля sca_o_divis_txt
        s.execute(sqlalchemy.text("""
UPDATE im.mt_stock_categ_calc_enrich t1
SET sca_o_divis_txt = COALESCE(t2.div_id_full_txt, t2.div_id::text)
FROM cdm.v_dim_divis t2
WHERE t1.sca_o_divis = t2.div_id 
and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month';
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 12 - Update поля sca_r_divis_txt
        s.execute(sqlalchemy.text("""
UPDATE im.mt_stock_categ_calc_enrich t1
SET sca_r_divis_txt = COALESCE(t2.div_id_full_txt, t2.div_id::text)
FROM cdm.v_dim_divis t2
WHERE t1.sca_r_divis = t2.div_id
and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month';
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        s.commit()

        # Шаг 13 - Update поля sca_or_divis_txt
        s.execute(sqlalchemy.text("""
UPDATE im.mt_stock_categ_calc_enrich t1
SET sca_or_divis_txt = t2.or_div_txt
FROM im.divis t2
WHERE t1.sca_o_divis = t2.o_div_id and t1.sca_r_divis = t2.r_div_id
and sca_date >= '{sca_date}'::date and sca_date < '{sca_date}'::date + interval '1 month';
""".format(sca_date=curr_month.strftime('%Y-%m-%d'))))
        
        # Закрываем коннект к бд
        s.commit()
        engine.dispose()


sample_var = PythonOperator(task_id='sample_var',
                               provide_context=True,  # Разрешает всем задачам в даге использовать глобальные переменные данного оператора
                               python_callable=python_sample_var,
                               dag=dag)

sample_var
