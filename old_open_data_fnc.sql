select * from organizations_meta limit 100

create table organizations_meta
(
	"ИНН" varchar(15),
	"СвАдресЮЛ.СвМНЮЛ.ИдНом" varchar(15),
	"СвАдресЮЛ.СвМНЮЛ.Регион" int,
	"СвАдресЮЛ.СвМНЮЛ.НаимРегион" varchar(100),
	"СвАдресЮЛ.СвМНЮЛ.МуниципРайон.ВидКод" int,
	"СвАдресЮЛ.СвМНЮЛ.МуниципРайон.Наим" varchar(100),
	"СвАдресЮЛ.СвМНЮЛ.ГородСелПоселен.ВидКод" int,
	"СвАдресЮЛ.СвМНЮЛ.ГородСелПоселен.Наим" varchar(100),
	"СвАдресЮЛ.СвМНЮЛ.НаселенПункт.Вид" varchar(10),
	"СвАдресЮЛ.СвМНЮЛ.НаселенПункт.Наим" varchar(100),
	"СвАдресЮЛ.СвМНЮЛ.ГРНДата.ГРН" varchar(15),
	"СвАдресЮЛ.СвМНЮЛ.ГРНДата.ДатаЗаписи" date,
	"СвАдресЮЛ.СвАдрЮЛФИАС.ИдНом" int,
	"СвАдресЮЛ.СвАдрЮЛФИАС.Индекс" int,
	"СвАдресЮЛ.СвАдрЮЛФИАС.Регион" int,
	"СвАдресЮЛ.СвАдрЮЛФИАС.НаимРегион" varchar(100),
	"СвАдресЮЛ.СвАдрЮЛФИАС.МуниципРайон.ВидКод" int,
	"СвАдресЮЛ.СвАдрЮЛФИАС.МуниципРайон.Наим" varchar(100),
	"СвАдресЮЛ.СвАдрЮЛФИАС.ГородСелПоселен.ВидКод" int,
	"СвАдресЮЛ.СвАдрЮЛФИАС.ГородСелПоселен.Наим" varchar(100),
	"СвАдресЮЛ.СвАдрЮЛФИАС.НаселенПункт.Вид" varchar(10),
	"СвАдресЮЛ.СвАдрЮЛФИАС.НаселенПункт.Наим" varchar(100),
	"СвАдресЮЛ.СвАдрЮЛФИАС.ЭлУлДорСети.Тип" varchar(10),
	"СвАдресЮЛ.СвАдрЮЛФИАС.ЭлУлДорСети.Наим" varchar(100),
	"СвАдресЮЛ.СвАдрЮЛФИАС.Здание.Тип" varchar(10),
	"СвАдресЮЛ.СвАдрЮЛФИАС.Здание.Номер" int,
	"СвАдресЮЛ.СвАдрЮЛФИАС.ПомещЗдания.Тип" varchar(10),
	"СвАдресЮЛ.СвАдрЮЛФИАС.ПомещЗдания.Номер" int,
	"СвАдресЮЛ.СвАдрЮЛФИАС.ГРНДата.ГРН" varchar(15),
	"СвАдресЮЛ.СвАдрЮЛФИАС.ГРНДата.ДатаЗаписи" date
)

select "Отрасль" as otrasl,
	"Страна" as country,
	"ОбщиеИнвестиции" as investingSum,
	"ВложенныеИнвестицииДо2021" as investing2021,
	"ЗапланированныеВ2022Инвестиции" as investing2022,
	"ОстатокИнвестиций" as restInvesting,
	"Номер" as num,
	1 as sluzhb
from econom_advisor

-- Список всех возможных ОГРН и ИНН из 39 и мета
-- Это у нас готов спраочник, его потом закинуть в форсайт как НСИ
-- Этот спраовчник нужно будет пересоздать после того, как 39 полностью загрузится
create table spr_big_ogrn as (
select ogrn_inn.ogrn, ogrn_inn.inn, inn_name_org.name_org, row_number() over() as row_num from
(select "ОГРН" as ogrn , "ИНН" as inn from organizations_meta group by 1, 2) ogrn_inn
left join
-- Список всех возможных наименований организаций и ИНН из всех таблиц и мета
(select "СведНП.НаимОрг" as name_org, "СведНП.ИННЮЛ" as inn from open_data_fnc_table_72 group by 1, 2
union
select "СведНП.НаимОрг" as name_org, "СведНП.ИННЮЛ" as inn from open_data_fnc_table_73 group by 1, 2
union
select "СведНП.НаимОрг" as name_org, "СведНП.ИННЮЛ" as inn from open_data_fnc_table_75 group by 1, 2
union
select "СведНП-НаимОрг" as name_org, "СведНП-ИННЮЛ" as inn from open_data_fnc_table_76 group by 1, 2
union
select "СведНП-НаимОрг" as name_org, "СведНП-ИННЮЛ" as inn from open_data_fnc_table_77 group by 1, 2
union
select "СведНП.НаимОрг" as name_org, "СведНП.ИННЮЛ" as inn from open_data_fnc_table_78 group by 1, 2
union
select "СведНП.НаимОрг" as name_org, "СведНП.ИННЮЛ" as inn from open_data_fnc_table_85 group by 1, 2
union
select "СвЮЛ.НаимОрг" as name_org, "СвЮЛ.ИННЮЛ" as inn from open_data_fnc_table_86 group by 1, 2) inn_name_org
on ogrn_inn.inn = inn_name_org.inn
group by 1, 2, 3
);
delete from spr_big_ogrn s1
using spr_big_ogrn s2
where s1.ogrn = s2.ogrn and s1.ctid > s2.ctid;

create table spr_okvad as (
	select okvad_code, okvad_name, okvad_vers, row_number() over() as row_num from (
		select "СвОКВЭД.КодОКВЭД" as okvad_code,
			"СвОКВЭД.НаимОКВЭД" as okvad_name,
			"СвОКВЭД.ПрВерсОКВЭД"::varchar(10) as okvad_vers from organizations_meta group by 1, 2, 3
		union
		select "СвОКВЭД.СвОКВЭДОсн.КодОКВЭД" as okvad_code,
			"СвОКВЭД.СвОКВЭДОсн.НаимОКВЭД" as okvad_name,
			"СвОКВЭД.СвОКВЭДОсн.ВерсОКВЭД"::varchar(10) as okvad_vers from open_data_fnc_table_39 group by 1, 2, 3
) t);
drop table spr_okvad;

-- Справочник регионов, залить в форсайт потом
create table spr_region as (
select reg_code.region_code, reg_name.region_type, reg_name.region_name, row_number() over() as row_num from
(select "СведМН.КодРегион" as region_code from open_data_fnc_table_39 group by 1
union select "КодРегион" as region_code from organizations_meta group by 1) as reg_code
left join
(select "СведМН.КодРегион" as region_code, "СведМН.Регион.Тип" as region_type, "СведМН.Регион.Наим" as region_name from open_data_fnc_table_39 group by 1, 2, 3) reg_name
on reg_code.region_code = reg_name.region_code
	);
delete from spr_region s1
using spr_region s2
where s1.region_code = s2.region_code and s1.ctid < s2.ctid;

-- Это вспомогательная табла, где вместе идет маппинг региона и инн
create table helper_table_region_inn as (
select reg_code.region_code, reg_name.region_type, reg_name.region_name, reg_code.inn, row_number() over() as row_num from
(select "СведМН.КодРегион" as region_code, "ОргВклМСП.ИННЮЛ" as inn from open_data_fnc_table_39 group by 1, 2
union select "КодРегион" as region_code, "ИНН" as inn from organizations_meta group by 1, 2) as reg_code
left join
(select "СведМН.КодРегион" as region_code, "СведМН.Регион.Тип" as region_type, "СведМН.Регион.Наим" as region_name from open_data_fnc_table_39 group by 1, 2, 3) reg_name
on reg_code.region_code = reg_name.region_code
);
delete from helper_table_region_inn s1
using helper_table_region_inn s2
where s1.inn = s2.inn and s1.ctid < s2.ctid;

create table helper_table_okvad_inn as (
	select "ИНН" as inn, "СвОКВЭД.КодОКВЭД" as okvad from organizations_meta group by 1, 2
);
delete from helper_table_okvad_inn s1
using helper_table_okvad_inn s2
where s1.inn = s2.inn and s1.ctid < s2.ctid

-- t1111 - Справочник огрн инн название, t2222 - справочник регионов
-- Плоские наборы
select t6609."KEY" as ogrn_id,
	t6618."KEY" as region_id,
	t7197."KEY" as okvad_id,
	t75."СведДохРасх.СумДоход" as income_75,
	t75."СведДохРасх.СумРасход" as expenses_75,
	t78."СведНаруш.СумШтраф" as fine_78,
	t85."СведССЧР.КолРаб" as employees_85
from t6609
left join open_data_fnc_table_72 t72 on t72."СведНП.ИННЮЛ" = t6609.inn
left join open_data_fnc_table_73 t73 on t73."СведНП.ИННЮЛ" = t6609.inn
left join open_data_fnc_table_75 t75 on t75."СведНП.ИННЮЛ" = t6609.inn
left join open_data_fnc_table_78 t78 on t78."СведНП.ИННЮЛ" = t6609.inn
left join open_data_fnc_table_85 t85 on t85."СведНП.ИННЮЛ" = t6609.inn
left join helper_table_region_inn on helper_table_region_inn.inn = t6609.inn
left join helper_table_okvad_inn on helper_table_okvad_inn.inn = t6609.inn
left join t6618 on t6618.name::int = helper_table_region_inn.region_code
left join t7197 on t7197.code = helper_table_okvad_inn.okvad
where t75."СведДохРасх.СумДоход" is not null
	or t75."СведДохРасх.СумРасход" is not null
	or t78."СведНаруш.СумШтраф" is not null
	or t85."СведССЧР.КолРаб" is not null

-- Таблица фактов для наборов 76-77
select t6618."KEY" as region_id,
	t6609."KEY" as ogrn_id,
	t6649."KEY" as nalog_id,
	t7197."KEY" as okvad_id,
	"СвУплСумНал.СумУплНал" as sum_nal,
	"СведНедоим.СумНедНалог" as nedo_nal,
	"СведНедоим.СумПени" as peni,
	"СведНедоим.СумШтраф" as shtraf,
	"СведНедоим.ОбщСумНедоим" as sum_nedo
from t6609
left join open_data_fnc_table_76 t76 on t76."СведНП-ИННЮЛ" = t6609.inn
left join open_data_fnc_table_77 t77 on t77."СведНП-ИННЮЛ" = t6609.inn
left join helper_table_region_inn on helper_table_region_inn.inn = t6609.inn
left join helper_table_okvad_inn on helper_table_okvad_inn.inn = t6609.inn
left join t6618 on t6618.name::int = helper_table_region_inn.region_code
left join t7197 on t7197.code = helper_table_okvad_inn.okvad
left join t6649 on t6649.name = t76."СвУплСумНал.НаимНалог"
where "СвУплСумНал.СумУплНал" is not null
	or "СведНедоим.СумНедНалог" is not null
	or "СведНедоим.СумПени" is not null
	or "СведНедоим.СумШтраф" is not null
	or "СведНедоим.ОбщСумНедоим" is not null
limit 100


create table open_data_fnc_table_63
(
	"СвРАФП-СвИЮЛАкт-НаимИЮЛ-НаимИЮЛПолн" text,
	"СвРАФП-СвИЮЛАкт-НаимИЮЛ-НаимИЮЛЛат" text,
	"СвРАФП-СвИЮЛАкт-НаимИЮЛ-НаимИЮЛСокр" text,
	"СвРАФП-СвИЮЛАкт-НаимИЮЛ-ДатаВнесРАФП" date,
	"СвРАФП-СвИЮЛАкт-АдрИЮЛИнк-СтрРег" int,
	"СвРАФП-СвИЮЛАкт-АдрИЮЛИнк-НаимСтр" varchar(50),
	"СвРАФП-СвИЮЛАкт-АдрИЮЛИнк-АдрИЮЛРег" text,
	"СвРАФП-СвИЮЛАкт-АдрИЮЛИнк-ДатаВнесРАФП" date,
	"СвРАФП-СвИЮЛАкт-РегИЮЛИнк-НаимРегОрг" text,
	"СвРАФП-СвИЮЛАкт-РегИЮЛИнк-РегНомер" text,
	"СвРАФП-СвИЮЛАкт-РегИЮЛИнк-ОсобРежРег" text,
	"СвРАФП-СвИЮЛАкт-РегИЮЛИнк-ДатаВнесРАФП" date,
	"СвРАФП-СвИЮЛАкт-КодИЮЛИнк-КодНП" text,
	"СвРАФП-СвИЮЛАкт-КодИЮЛИнк-ДатаВнесРАФП" date,
	"СвРАФП-СвИЮЛАкт-УставИЮЛ-УстКап" bigint,
	"СвРАФП-СвИЮЛАкт-УставИЮЛ-КодВалют" int,
	"СвРАФП-СвИЮЛАкт-УставИЮЛ-ДатаВнесРАФП" date,
	"СвРАФП-СвАФПАкт-НаимАФП-НаимАФППолн" text,
	"СвРАФП-СвАФПАкт-НаимАФП-НаимАФПСокр" text, 
	"СвРАФП-СвАФПАкт-НаимАФП-ДатаВнесРАФП" date,
	"СвРАФП-СвАФПАкт-АдрАФПРФ" ,
	"СвРАФП-СвАФПАкт-АдрАФПРФ" ,
	"СвРАФП-СвАФПАкт-АдрАФПРФ" ,
	"СвРАФП-СвАФПАкт-" ,
	"СвРАФП-СвАФПАкт-" ,
	"СвРАФП-СвАФПАкт-" ,
	"СвРАФП-СвАФПАкт-" ,
)

