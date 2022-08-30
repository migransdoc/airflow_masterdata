CREATE SERVER test_csv_part2_new_format FOREIGN DATA WRAPPER file_fdw;
drop table data_norm_part2_new_format;

create table data_norm_part2_new_format (
	"Головная (да нет)" varchar(3),
	"ИНН головной организации" varchar(20),
	"ОКПО" varchar(20),
	"Полное наименование организации" text,
	"ИНН" varchar(20),
	"ОГРН" varchar(30),
	"ОКФС" varchar(30),
	"ОКВЭД2" varchar(20),
	"Учредители акционеры" text,
	"Субъект Российской Федерации" varchar(100),
	date date,
	value_total_pfr text,  -- общая численность работников по версии ПФР
	value_total_prr text,  -- По версии ПРР
	value_total_diff text,  -- Разница версий
	value_firing text,  -- Работники к увольнению
	value_vacation text,  -- Работники в отпуске без сохранения зп
	value_rest text,  -- Работники в простое
	value_parttime text, -- Работники, работающие неполный рабочий день
	value_distant text  -- Работники удаленно
);

create index i_data_norm_gol_part2 on data_norm_part2_new_format("Головная (да нет)");
create index i_data_norm_inn_gol_part2 on data_norm_part2_new_format("ИНН головной организации");
create index i_data_norm_okpo_part2 on data_norm_part2_new_format("ОКПО");
create index i_data_norm_nameorg_part2 on data_norm_part2_new_format("Полное наименование организации");
create index i_data_norm_inn_new_format_part2 on data_norm_part2_new_format("ИНН");
create index i_data_norm_orgn_part2 on data_norm_part2_new_format("ОГРН");
create index i_data_norm_okfs_part2 on data_norm_part2_new_format("ОКФС");
create index i_data_norm_okvad2_part2 on data_norm_part2_new_format("ОКВЭД2");
create index i_data_norm_founders_part2 on data_norm_part2_new_format("Учредители акционеры");
create index i_data_norm_reg_part2 on data_norm_part2_new_format("Субъект Российской Федерации");
create index i_data_norm_date_new_format_part2 on data_norm_part2_new_format(date);

truncate table data_norm_part2_new_format;
truncate table data_norm_new_format;

create or replace procedure refresh_data_part2_new_format ()
  language plpgsql
as
$$
declare
  l_str text;
begin

  drop FOREIGN table if exists new_data_part2_new_format;
  drop foreign table if exists test_csv_part2_new_format;

	CREATE FOREIGN TABLE test_csv_part2_new_format (
		str text
	  ) SERVER test_csv_part2_foreign_new_format
	  OPTIONS ( filename '../../../../../temp/2nd.csv', format 'text' );
	  
	--create table test_csv_part2 as select * from test_csv_part2_foreign;
	  
	--update test_csv_part2
	--set str = regexp_replace(str, ';', '', 'i');

	-- Рабочая версия
	select  replace(replace(replace(replace(replace('CREATE FOREIGN TABLE new_data_part2_new_format (' || string_agg('"' || concat_rows || '" text', ',') || ') SERVER test_csv_part2_new_format OPTIONS ( filename ''../../../../../temp/2nd.csv'', format ''csv'', delimiter '';'', QUOTE ''"'' );', 'Численность работников, предполагаемых к увольнению, чел', 'Работники к увольнению'),
											'Численность работников, находящихся в отпусках без сохранения платы, чел', 'Работники в отпуске'),
									'Численность работников, находящихся в простое, чел', 'Работники в простое'),
							'Численность работников, работающих неполный рабочий день, чел', 'Работники неполный день'),
					'Численность работников, работающих на удаленной работе, чел', 'Работники удаленно')
	into l_str
	from (
	-- Рабочая версия готовых колонок
	select case when first_row.str2 = '' then second_row.str else concat(second_row.str, ' ', first_row.str2) end concat_rows, second_row.rn
	from (
		with dtb as (
		select  trim(tb.str) as str,
				tb.rn
		from    unnest(
		  (select  string_to_array(str, ';')
		  from    test_csv_part2_new_format
		  limit 1 offset 1)
		) WITH ORDINALITY AS tb (str, rn)
	  )
	 select coalesce(left(dtb2.str, 10) || '_', '') || dtb1.str as str, dtb1.rn from (select * from dtb) dtb1
	 left join (select * from dtb) dtb2 on dtb1.str like 'разница ПФР и РВР'
							and dtb2.rn = dtb1.rn-1
	) second_row
	join (
		select rn, case when str2 in ('﻿', 'Численность работников предприятия, чел.') then '' else str2 end
	from (
	with dtb as (
		select  trim(tb.str) as str,
				tb.rn
		from    unnest(
		  (select  string_to_array(str, ';')
		  from    test_csv_part2_new_format
		  limit 1)
		) WITH ORDINALITY AS tb (str, rn)
	  )
	select str, rn, first_value(str) over (partition by str1) as str2
	from (
		select str, rn, count(case when str != '' then 1 end) over (order by rn) as str1
		from dtb
	) t --order by rn
	order by rn) t1
	) first_row on second_row.rn = first_row.rn
	) t;

	execute l_str;

	  with dtb as (
    select  atr.attname
    from    pg_namespace ns
    join    pg_class cl on cl.relnamespace = ns.oid
                        and cl.relispartition = false
    join    pg_attribute atr on atr.attrelid = cl.oid
                            and not atr.attisdropped
                            and atr.attnum > 10
    join    pg_type t on t.oid = atr.atttypid
    where   ns.nspname = 'public'
    and     cl.relname = 'new_data_part2_new_format'
    order by atr.attnum
  )
  select  string_agg('(to_date(''' || left(dtb.attname, 10) || ''', ''dd.mm.yyyy''), t."' || dtb.attname || '", '|| 't."'||dtb1.attname||'", ' || 't."'||dtb2.attname||'", ' || 't."'||dtb3.attname||'", ' || 't."'||dtb4.attname||'", ' || 't."'||dtb5.attname||'", ' || 't."'||dtb6.attname||'", ' || 't."' || dtb7.attname || '")' , ',')
  into l_str
  from    dtb
  left join dtb as dtb1 on dtb1.attname = left(dtb.attname, 10) || ' РВР'
  left join dtb as dtb2 on dtb2.attname = left(dtb.attname, 10) || ' разница ПФР и РВР'
  left join dtb as dtb3 on dtb3.attname = to_date(dtb.attname, 'dd.mm.yyyy')::text  || ' Работники к увольнению'
  left join dtb as dtb4 on dtb4.attname = to_date(dtb.attname, 'dd.mm.yyyy')::text  || ' Работники в отпуске'
  left join dtb as dtb5 on dtb5.attname = to_date(dtb.attname, 'dd.mm.yyyy')::text  || ' Работники в простое'
  left join dtb as dtb6 on dtb6.attname = to_date(dtb.attname, 'dd.mm.yyyy')::text  || ' Работники неполный день'
  left join dtb as dtb7 on dtb7.attname = to_date(dtb.attname, 'dd.mm.yyyy')::text  || ' Работники удаленно'
  where dtb.attname like '%ПФР';

		truncate table data_norm_part2_new_format;

	l_str := '
    insert into data_norm_part2_new_format
      select
	  		  t."Головная (данет)",
			  t."ИНН головной организации",
			  t."ОКПО",
			  t."Полное наименование организации",
			  t."ИНН",
			  t."ОГРН",
			  t."ОКФС",
			  t."ОКВЭД2",
			  t."Учредители/акционеры (доля участи",
			  t."Субъект Российской Федерации",
              tb.date,
              tb.value_total_pfr,
              tb.value_total_prr,
              tb.value_total_diff,
			  tb.value_firing,
			  tb.value_vacation,
			  tb.value_rest,
			  tb.value_parttime,
			  tb.value_distant
      from    (
          select *
          from new_data_part2_new_format
          offset 3) t
      cross join lateral (
         values '||l_str||'
         ) as tb (date, value_total_pfr, value_total_prr, value_total_diff, value_firing, value_vacation, value_rest, value_parttime, value_distant)';

	alter table data_norm_part2_new_format alter column value_total_pfr type text using value_total_pfr::text;
	alter table data_norm_part2_new_format alter column value_total_prr type text using value_total_prr::text;
	alter table data_norm_part2_new_format alter column value_total_diff type text using value_total_diff::text; -- тут ошибка
	alter table data_norm_part2_new_format alter column value_firing type text using value_firing::text;
	alter table data_norm_part2_new_format alter column value_vacation type text using value_vacation::text;
	alter table data_norm_part2_new_format alter column value_rest type text using value_rest::text;
	alter table data_norm_part2_new_format alter column value_parttime type text using value_parttime::text;
	alter table data_norm_part2_new_format alter column value_distant type text using value_distant::text;
	
	execute l_str;
	
	alter table data_norm_part2_new_format alter column value_total_pfr type numeric using value_total_pfr::numeric;
	alter table data_norm_part2_new_format alter column value_total_prr type numeric using value_total_prr::numeric;
	alter table data_norm_part2_new_format alter column value_total_diff type int using value_total_diff::numeric; -- тут ошибка
	alter table data_norm_part2_new_format alter column value_firing type numeric using value_firing::numeric;
	alter table data_norm_part2_new_format alter column value_vacation type numeric using value_vacation::numeric;
	alter table data_norm_part2_new_format alter column value_rest type numeric using value_rest::numeric;
	alter table data_norm_part2_new_format alter column value_parttime type numeric using value_parttime::numeric;
	alter table data_norm_part2_new_format alter column value_distant type numeric using value_distant::numeric;

	update data_norm_part2_new_format
	set "Учредители акционеры" = case
		when position('Россия(0.000%)' in "Учредители акционеры") > 0 then 'Иностранная собственность'
		when position('Россия(100.000%)' in "Учредители акционеры") > 0 then 'Российская собственность'
		else 'Совместная российская и иностранная собственность' end;

	  -- Обновление справочников
	  truncate t8646;
	  insert into t8646("KEY", name, ord)
	  select row_number() over(), "Головная (да нет)", row_number() over() from data_norm_part2_new_format
	  group by 2;

	   truncate t8649;
	  insert into t8649("KEY", name, ord)
	  select row_number() over(), date, row_number() over() from data_norm_part2_new_format
	  group by 2;

	  truncate t8651;
	  insert into t8651("KEY", name, ord, head_inn, okpo, inn, ogrn)
	  select row_number() over(), "Полное наименование организации", row_number() over(), "ИНН головной организации", "ОКПО", "ИНН", "ОГРН" from data_norm_part2_new_format
	  group by 2, 4, 5, 6, 7;

	alter table data_norm_part2_new_format alter column "ОКВЭД2" type text;
	update data_norm_part2_new_format
set "ОКВЭД2" = case left("ОКВЭД2", 3)
	when '29.' then 'Производство автотранспортных средств, прицепов и полуприцепов'
	when '47.' then 'Торговля розничная, кроме торговли автотранспортными средствами и мотоциклами'
	when '56.' then 'Деятельность по предоставлению продуктов питания и напитков'
	when '52.' then 'Складское хозяйство и вспомогательная транспортная деятельность'
	when '30.' then 'Производство прочих транспортных средств и оборудования'
	when '16.' then 'Обработка древесины и производство изделий из дерева и пробки, кроме мебели, производство изделий из соломки и материалов для плетения'
	when '27.' then 'Производство электрического оборудования'
	when '22.' then 'Производство резиновых и пластмассовых изделий'
	when '25.' then 'Производство готовых металлических изделий, кроме машин и оборудования'
	else 'Другое' end;	
	  truncate t8653;
insert into t8653("KEY", name, ord)
select row_number() over(), "ОКВЭД2", row_number() over() from data_norm_part2_new_format
group by 2;

	   truncate t8655;
insert into t8655("KEY", name, ord)
select row_number() over(), "ОКФС", row_number() over() from data_norm_part2_new_format
group by 2;

	   truncate t8657;
insert into t8657("KEY", name, ord)
select row_number() over(), "Субъект Российской Федерации", row_number() over() from data_norm_part2_new_format
group by 2;
  
	   truncate t8659;
insert into t8659("KEY", name, ord)
	select row_number() over(), "Учредители акционеры", row_number() over() from data_norm_part2_new_format
	group by 2;
	
end;
$$;	

call refresh_data_part2_new_format();
truncate table data_norm_part2_new_format;
select * from data_norm_part2_new_format limit 1000;
select count(*) from data_norm_part2_new_format


create user postgres1 superuser
SELECT usename AS role_name,
  CASE 
     WHEN usesuper AND usecreatedb THEN 
	   CAST('superuser, create database' AS pg_catalog.text)
     WHEN usesuper THEN 
	    CAST('superuser' AS pg_catalog.text)
     WHEN usecreatedb THEN 
	    CAST('create database' AS pg_catalog.text)
     ELSE 
	    CAST('' AS pg_catalog.text)
  END role_attributes
FROM pg_catalog.pg_user
ORDER BY role_name desc;
alter user postgres1 password 'postgres1'




