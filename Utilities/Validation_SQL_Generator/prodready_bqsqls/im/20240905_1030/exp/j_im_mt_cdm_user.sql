-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_mt_cdm_user.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT t3.company_code AS company_code,
          t3.coid AS coid,
          t2.mt_user_mnemonic_cs AS mt_user_mnemonic_cs,
          t1.mt_user_id AS mt_user_id,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
   FROM {{ params.param_im_stage_dataset_name }}.mt_cdm_user_3_4 AS t1
   INNER JOIN {{ params.param_im_stage_dataset_name }}.mt_cdm_user_mnem AS t2 ON t1.role_plyr_sk = t2.role_plyr_sk
   INNER JOIN {{ params.param_im_stage_dataset_name }}.mt_cdm_user_coid AS t3 ON t1.role_plyr_sk = t3.role_plyr_sk) AS a