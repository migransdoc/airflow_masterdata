CREATE EXTENSION file_fdw;
CREATE SERVER test_csv FOREIGN DATA WRAPPER file_fdw;

create table data_norm_new_format (
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
	value_total int,
	value_in int,
	invalue_out int
);

create index i_data_norm_gol on data_norm_new_format("Головная (да нет)");
create index i_data_norm_inn_gol on data_norm_new_format("ИНН головной организации");
create index i_data_norm_okpo on data_norm_new_format("ОКПО");
create index i_data_norm_nameorg on data_norm_new_format("Полное наименование организации");
create index i_data_norm_inn_new_format on data_norm_new_format("ИНН");
create index i_data_norm_orgn on data_norm_new_format("ОГРН");
create index i_data_norm_okfs on data_norm_new_format("ОКФС");
create index i_data_norm_okvad2 on data_norm_new_format("ОКВЭД2");
create index i_data_norm_founders on data_norm_new_format("Учредители акционеры");
create index i_data_norm_reg on data_norm_new_format("Субъект Российской Федерации");
create index i_data_norm_date_new_format on data_norm_new_format(date);


create or replace procedure refresh_data_new_format ()
  language plpgsql
as
$$
declare
  l_str text;
begin

  drop FOREIGN table if exists test_csv_new_format;
  drop FOREIGN table if exists new_data_new_format;

  CREATE FOREIGN TABLE test_csv_new_format (
    str text
  ) SERVER test_csv
  OPTIONS ( filename '../../../../../temp/1.csv', format 'text' );

  with dtb as (
    select  trim(tb.str) as str,
            tb.rn
    from    unnest(
      (select  string_to_array(str, ';')
      from    test_csv
      limit   1)
    ) WITH ORDINALITY AS tb (str, rn)
  )
  select  'CREATE FOREIGN TABLE new_data_new_format ('||string_agg('"'||coalesce(dtb1.str||'_', '')||dtb.str||'" text', ',')||') SERVER test_csv OPTIONS ( filename ''../../../../../temp/1.csv'', format ''csv'', delimiter '';'', QUOTE ''"'' );'
  into    l_str
  from    dtb
  left join dtb as dtb1 on dtb.str = 'принято'
                        and dtb1.rn = dtb.rn-1
                        or  dtb.str = 'уволено'
                        and dtb1.rn = dtb.rn-2;

  EXECUTE l_str;

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
    and     cl.relname = 'new_data_new_format'
    order by atr.attnum
  )
  select  string_agg('(to_date('''||dtb.attname||''', ''dd.mm.yyyy''), t."'||dtb.attname||'", '||coalesce('t."'||dtb1.attname||'"', 'null')||', '||coalesce('t."'||dtb2.attname||'"', 'null')||')', ',')
  into    l_str
  from    dtb
  left join dtb as dtb1 on dtb1.attname = dtb.attname||'_принято'
  left join dtb as dtb2 on dtb2.attname = dtb.attname||'_уволено'
  where   dtb.attname not like '%принято'
  and     dtb.attname not like '%уволено';

	  truncate table data_norm_new_format;
  l_str := '
	insert into data_norm_new_format
      select  t."Головная (данет)",
			  t."ИНН головной организации",
			  t."ОКПО",
			  t."Полное наименование организации",
			  t."ИНН",
			  t."ОГРН",
			  t."ОКФС"::text,
			  t."ОКВЭД2",
			  t."Учредители/акционеры (доля участи",
			  t."Субъект Российской Федерации",
			  tb.date,
			  tb.value_total::int,
			  tb.value_in::int,
			  tb.value_out::int
			  
      from    (
          select *
          from new_data_new_format
          where   "Головная (данет)" is not null
          offset 1) t
      cross join lateral (
         values  '||l_str||'
         ) as tb (date, value_total, value_in, value_out)';

  EXECUTE l_str;
  
  update data_norm_new_format
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
  
  	alter table data_norm_new_format alter column "ОКВЭД2" type text;
  	update data_norm_new_format
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
select row_number() over(), "ОКВЭД2", row_number() over() from data_norm_new_format
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

truncate data_norm_new_format;
call  refresh_data_new_format ();
select * from data_norm_part2_new_format

select * from data_norm_new_format limit 100


select  date,
        sum(value_total),
        sum(value_in),
        sum(value_out)
from data_norm
group by date
order by date;




