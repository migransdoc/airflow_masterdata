create view im.v_potr(rsnum, rspos, lgort, obj_date, obj_year, obj_month, matnr, rs_qty_avlb, matkl, proizv, o_divis, r_divis, werks, moni_gr, saknr, bwtar, unit, zvd, ekgrp, pt_fp01, pt_qty_res, potr_stad, o_divis_txt, r_divis_txt, wm_or_txt, sdp_qty, sdp_amt, sat_flag, pla_flag, matnr_txt, matkl_txt, pt_qty_otpu, pt_amt_otpu, pt_qty_open, pt_amt_open, pt_manu_date, pt_modul, pt_section) as
	SELECT t2.rsnum,
       t2.rspos,
       t2.lgort,
       t2.obj_date,
       t2.obj_year,
       t2.obj_month,
       t2.matnr,
       t2.rs_qty_avlb,
       t2.matkl,
       t2.proizv,
       t2.o_divis,
       t2.r_divis,
       t2.werks,
       t2.moni_gr,
       t2.saknr,
       t2.bwtar,
       t2.unit,
       t2.zvd,
       t2.ekgrp,
       t2.pt_fp01,
       t2.pt_qty_res,
       t2.potr_stad,
       t2.o_divis_txt,
       t2.r_divis_txt,
       t2.wm_or_txt,
       t2.sdp_qty,
       t2.sdp_qty * (t2.pt_fp01 / t2.pt_qty_res) AS sdp_amt,
       false                                     AS sat_flag,
       false                                     AS pla_flag,
       t2.matnr_txt,
       t2.matkl_txt,
       t2.pt_qty_otpu,
       t2.pt_amt_otpu,
       t2.pt_qty_open,
       t2.pt_amt_open,
       t2.pt_manu_date,
       t2.pt_modul,
       t2.pt_section
FROM (SELECT t1.rsnum,
             t1.rspos,
             t1.lgort,
             t1.obj_date,
             t1.obj_year,
             t1.obj_month,
             t1.matnr,
             t1.rs_qty_avlb,
             t1.matkl,
             t1.proizv,
             t1.o_divis,
             t1.r_divis,
             t1.werks,
             t1.moni_gr,
             t1.saknr,
             t1.bwtar,
             t1.unit,
             t1.zvd,
             t1.ekgrp,
             t1.pt_fp01,
             t1.pt_qty_res,
             t1.potr_stad,
             t1.o_divis_txt,
             t1.r_divis_txt,
             t1.wm_or_txt,
             CASE t1.potr_stad
                 WHEN 8 THEN COALESCE(t1.pt_qty_open, 0::numeric) - COALESCE(t1.rs_qty_avlb, 0::numeric)
                 WHEN 9 THEN
                     CASE
                         WHEN COALESCE(t1.rs_qty_avlb, 0::numeric) >= COALESCE(t1.pt_qty_res, 0::numeric) THEN 0::numeric
                         ELSE COALESCE(t1.pt_qty_open, 0::numeric) - COALESCE(t1.rs_qty_avlb, 0::numeric)
                         END
                 ELSE NULL::numeric
                 END AS sdp_qty,
             t1.matnr_txt,
             t1.matkl_txt,
             t1.pt_qty_otpu,
             t1.pt_amt_otpu,
             t1.pt_qty_open,
             t1.pt_amt_open,
             t1.pt_manu_date,
             t1.pt_modul,
             t1.pt_section
      FROM (SELECT v_potr.pt_rsnum                               AS rsnum,
                   v_potr.pt_rspos                               AS rspos,
                   v_potr.pt_lgort                               AS lgort,
                   v_potr.pt_bdter                               AS obj_date,
                   v_potr.pt_year                                AS obj_year,
                   v_potr.pt_month                               AS obj_month,
                   v_potr.pt_matnr                               AS matnr,
                   sub.rs_qty_avlb,
                   v_mat_txt.mt_matkl                            AS matkl,
                   dim_wh_map.wm_proizv                          AS proizv,
                   dim_wh_map.wm_o_divis                         AS o_divis,
                   dim_wh_map.wm_r_divis                         AS r_divis,
                   v_potr.pt_werks                               AS werks,
                   v_mat_txt.mt_moni_gr                          AS moni_gr,
                   v_potr.pt_saknr                               AS saknr,
                   v_potr.pt_bwtar                               AS bwtar,
                   v_potr.pt_unit_qty                            AS unit,
                   v_potr.pt_zvd                                 AS zvd,
                   v_mat_txt.mt_ekgrp                            AS ekgrp,
                   v_potr.pt_fp01,
                   v_potr.pt_qty_res,
                   CASE
                       WHEN v_potr.pt_bwtar::text = 'СОБСТВЕН'::text AND ((v_potr.pt_pp_aufnr::text = ANY
                                                                           (ARRAY [''::character varying::text, '0'::character varying::text])) OR
                                                                          v_potr.pt_pp_aufnr IS NULL) THEN 8::smallint
                       WHEN v_potr.pt_bwtar::text = 'СОБСТВЕН'::text AND (v_potr.pt_pp_aufnr::text <> ALL
                                                                          (ARRAY [''::character varying::text, '0'::character varying::text])) AND
                            v_potr.pt_pp_aufnr IS NOT NULL THEN 9::smallint
                       ELSE NULL::smallint
                       END                                       AS potr_stad,
                   COALESCE(t4.div_id_full_txt, t4.div_id::text) AS o_divis_txt,
                   COALESCE(t5.div_id_full_txt, t5.div_id::text) AS r_divis_txt,
                   divis_tbl.or_div_txt                          AS wm_or_txt,
                   v_mat_txt.mt_cc_idtext                        AS matnr_txt,
                   t6.mtg_cc_idtext                              AS matkl_txt,
                   v_potr.pt_enmng                               AS pt_qty_otpu,
                   v_potr.pt_amt_otpu,
                   v_potr.pt_qty_open,
                   v_potr.pt_amt_open,
                   v_potr.pt_manu_date,
                   v_potr.pt_modul,
                   CASE
                       WHEN v_potr.pt_bwtar::text = 'ПОКУПНОЙ'::text THEN
                           CASE
                               WHEN v_potr.pt_modul = 1 THEN
                                   CASE
                                       WHEN v_potr.pt_saknr = 1003000000 THEN 'МАСЛА'::text
                                       ELSE 'СОДЕРЖАНИЕ'::text
                                       END
                               WHEN v_potr.pt_modul = 2 THEN 'РЕМОНТНЫЙ ФОНД'::text
                               WHEN v_potr.pt_modul = 3 THEN
                                   CASE
                                       WHEN v_potr.pt_saknr = 1003000000 THEN 'МАСЛА'::text
                                       ELSE 'ТЕХНОЛОГИЯ'::text
                                       END
                               ELSE NULL::text
                               END
                       WHEN v_potr.pt_bwtar::text = 'СОБСТВЕН'::text THEN 'СОБСТВЕН'::text
                       ELSE NULL::text
                       END                                       AS pt_section
            FROM edw.potr v_potr
                     JOIN edw.dim_mat_txt v_mat_txt ON v_potr.pt_matnr::text = v_mat_txt.mt_matnr::text AND
                                                       v_potr.pt_werks::text = v_mat_txt.mt_werks::text
                     LEFT JOIN edw.dim_wh_map ON v_potr.pt_lgort::text = dim_wh_map.wm_lgort::text AND
                                                 v_potr.pt_werks::text = dim_wh_map.wm_werks::text
                     LEFT JOIN cdm.v_dim_divis t4 ON dim_wh_map.wm_o_divis::text = t4.div_id::text
                     LEFT JOIN cdm.v_dim_divis t5 ON dim_wh_map.wm_r_divis::text = t5.div_id::text
                     LEFT JOIN edw.dim_mat_gr_txt t6 ON v_mat_txt.mt_matkl::text = t6.mtg_matkl::text
                     LEFT JOIN im.divis divis_tbl ON dim_wh_map.wm_o_divis::text = divis_tbl.o_div_id::text AND
                                                     dim_wh_map.wm_r_divis::text = divis_tbl.r_div_id::text
                     LEFT JOIN (SELECT v_reqstock.rs_rsnum,
                                       v_reqstock.rs_rspos,
                                       sum(v_reqstock.rs_qty_avlb) AS rs_qty_avlb
                                FROM cdm.v_reqstock
                                WHERE v_reqstock.rs_tech_data_type = 1
                                GROUP BY v_reqstock.rs_rsnum, v_reqstock.rs_rspos) sub
                               ON sub.rs_rsnum = v_potr.pt_rsnum AND sub.rs_rspos = v_potr.pt_rspos
            WHERE v_potr.pt_tech_data_type = 1
              AND (v_potr.pt_bwtar::text = ANY
                   (ARRAY ['ПОКУПНОЙ'::character varying::text, 'СОБСТВЕН'::character varying::text]))) t1) t2;

alter table im.v_potr owner to z_bi_consultant;

grant select on im.v_potr to z_read_only;

