create function make_chain_potr_log(p_load_id character varying) returns void
    language plpgsql
as
$$
declare
    temprow           edw.potr_log%rowtype; -- нужно для итерирования в цикле по исходной таблице
    chain_idl         int = 1; -- начальный айди для цепочек
    current_chain_id  int = 0; -- для операций "в моменте"
    least_chain_id    int;
    greatest_chain_id int;
begin
    chain_idl = (select coalesce(max(chain_id) + 1, 1) from edw.potr_log_chain);
    for temprow in -- главный цикл по исходной таблице, где идем по каждой записи
        select *
        from edw.potr_log
        where load_id = p_load_id
        loop
            if temprow.pl_rsnum_new = 0 and temprow.pl_rspos_new = 0 then -- если выходная потребность нулевая
                if not (select exists(select 1
                                      from edw.potr_log_chain
                                      where plc_rsnum = temprow.pl_rsnum
                                        and plc_rspos = temprow.pl_rspos)) then -- если начальной потребности нет в итог таблице
                    insert into edw.potr_log_chain (plc_rsnum, plc_rspos, plc_udate, plc_utime,
                                                    chain_id, plc_lchg_date_time)
                    values (temprow.pl_rsnum,
                            temprow.pl_rspos,
                            temprow.pl_udate,
                            temprow.pl_utime,
                            chain_idl,
                            now());
                    chain_idl = chain_idl + 1;
                end if;

            elsif not (select exists(select 1
                                     from edw.potr_log_chain
                                     where plc_rsnum = temprow.pl_rsnum
                                       and plc_rspos = temprow.pl_rspos)) and
                  not (select exists(select 1
                                     from edw.potr_log_chain
                                     where plc_rsnum = temprow.pl_rsnum_new
                                       and plc_rspos = temprow.pl_rspos_new)) then -- условие на проверку, что обе потребности из одной строки ориг таблицы не представлены
            -- в финальной таблице
                if temprow.pl_rsnum = temprow.pl_rsnum_new and
                   temprow.pl_rspos = temprow.pl_rspos_new then -- если потребность перетекает сама в себя
                    insert into edw.potr_log_chain (plc_rsnum, plc_rspos, plc_udate, plc_utime,
                                                    chain_id, plc_lchg_date_time)
                    values (temprow.pl_rsnum,
                            temprow.pl_rspos,
                            temprow.pl_udate,
                            temprow.pl_utime,
                            chain_idl,
                            now()); -- вставили потребность из текущей строки
                    chain_idl = chain_idl + 1; -- увеличили счетчик текущей цепи
                else -- если потребность разные
                    insert into edw.potr_log_chain (plc_rsnum, plc_rspos, plc_udate, plc_utime,
                                                    chain_id, plc_lchg_date_time)
                    values (temprow.pl_rsnum,
                            temprow.pl_rspos,
                            temprow.pl_udate,
                            temprow.pl_utime,
                            chain_idl,
                            now()); -- вставили первую потребность из текущей строки
                    insert into edw.potr_log_chain (plc_rsnum, plc_rspos, plc_udate, plc_utime,
                                                    chain_id, plc_lchg_date_time)
                    values (temprow.pl_rsnum_new,
                            temprow.pl_rspos_new,
                            temprow.pl_udate,
                            temprow.pl_utime,
                            chain_idl,
                            now()); --  вставили вторую потребность из текущей строки
                    chain_idl = chain_idl + 1; -- увеличили счетчик текущей цепи
                end if;

            elsif (select exists(select 1
                                 from edw.potr_log_chain
                                 where plc_rsnum = temprow.pl_rsnum
                                   and plc_rspos = temprow.pl_rspos)) and
                  not (select exists(select 1
                                     from edw.potr_log_chain
                                     where plc_rsnum = temprow.pl_rsnum_new
                                       and plc_rspos = temprow.pl_rspos_new)) then
                -- проверка, что первая потребность в строке есть в итог таблице, а вторая нет
                -- raise notice ;
                current_chain_id = (select chain_id
                                    from edw.potr_log_chain
                                    where plc_rsnum = temprow.pl_rsnum
                                      and plc_rspos = temprow.pl_rspos
                ); -- находим айди цепочки для первой потребности, чтоб записать в ту же цепь вторую
                insert into edw.potr_log_chain (plc_rsnum, plc_rspos, plc_udate, plc_utime, chain_id,
                                                plc_lchg_date_time)
                values (temprow.pl_rsnum_new,
                        temprow.pl_rspos_new,
                        temprow.pl_udate,
                        temprow.pl_utime,
                        current_chain_id,
                        now()); -- вставили вторую потребность в итог таблицу
                current_chain_id = 0;

            elsif not (select exists(select 1
                                     from edw.potr_log_chain
                                     where plc_rsnum = temprow.pl_rsnum
                                       and plc_rspos = temprow.pl_rspos)) and
                  (select exists(select 1
                                 from edw.potr_log_chain
                                 where plc_rsnum = temprow.pl_rsnum_new
                                   and plc_rspos = temprow.pl_rspos_new)) then
                -- проверка, что вторая потребность в строке есть в итог таблице, а первая нет
                -- raise notice ;
                current_chain_id = (select chain_id
                                    from edw.potr_log_chain
                                    where plc_rsnum = temprow.pl_rsnum_new
                                      and plc_rspos = temprow.pl_rspos_new
                ); -- находим айди цепочки для первой потребности, чтоб записать в ту же цепь вторую
                insert into edw.potr_log_chain (plc_rsnum, plc_rspos, plc_udate, plc_utime, chain_id,
                                                plc_lchg_date_time)
                values (temprow.pl_rsnum,
                        temprow.pl_rspos,
                        temprow.pl_udate,
                        temprow.pl_utime,
                        current_chain_id,
                        now()); -- вставили первую потребность в итог таблицу
                current_chain_id = 0;

            else -- конечное условие, что обе потребности из строки есть в конечной таблице
            --raise notice '%, %, %, %', temprow.pl_rsnum, temprow.pl_rspos, temprow.pl_rsnum_new, temprow.pl_rspos_new;
                if (select distinct chain_id
                    from edw.potr_log_chain
                    where plc_rsnum = temprow.pl_rsnum
                      and plc_rspos = temprow.pl_rspos) != (
                       select distinct chain_id
                       from edw.potr_log_chain
                       where plc_rsnum = temprow.pl_rsnum_new
                         and plc_rspos = temprow.pl_rspos_new
                   ) then -- проверка, в одной ли цепе находятся обе потребности
                -- если нет, то обе разных цепи сворачиваем в одну
                    least_chain_id = least((select chain_id
                                            from edw.potr_log_chain
                                            where plc_rsnum = temprow.pl_rsnum
                                              and plc_rspos = temprow.pl_rspos),
                                           (select chain_id
                                            from edw.potr_log_chain
                                            where plc_rsnum = temprow.pl_rsnum_new
                                              and plc_rspos = temprow.pl_rspos_new));
                    -- выше мы нашли айди минимальной цепочки из двух, в нее и будем переводить все
                    greatest_chain_id = greatest((select chain_id
                                                  from edw.potr_log_chain
                                                  where plc_rsnum = temprow.pl_rsnum
                                                    and plc_rspos = temprow.pl_rspos),
                                                 (select chain_id
                                                  from edw.potr_log_chain
                                                  where plc_rsnum = temprow.pl_rsnum_new
                                                    and plc_rspos = temprow.pl_rspos_new));
                    update edw.potr_log_chain
                    set chain_id           = least_chain_id,
                        plc_lchg_date_time = now()
                    where chain_id = greatest_chain_id; -- меняем цепочки
                end if;
            end if;
        end loop;
end ;
$$;

alter function make_chain_potr_log(varchar) owner to z_bi_consultant;

