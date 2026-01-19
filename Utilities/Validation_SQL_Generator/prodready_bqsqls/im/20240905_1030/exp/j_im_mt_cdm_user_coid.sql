-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_mt_cdm_user_coid.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT DISTINCT encnt_to_role.role_plyr_sk AS role_plyr_sk,
                   encnt_to_role.vld_to_ts AS vld_to_ts,
                   encnt_to_role.company_code AS company_code,
                   encnt_to_role.coid AS coid,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
   FROM {{ params.param_im_auth_base_views_dataset_name }}.encnt_to_role
   WHERE encnt_to_role.vld_to_ts = '9999-12-31 00:00:00'
     AND NOT upper(encnt_to_role.rl_type_ref_cd) LIKE 'INSUR%'
     AND NOT upper(encnt_to_role.rl_type_ref_cd) LIKE 'FACIL%'
     AND NOT upper(encnt_to_role.rl_type_ref_cd) LIKE 'PATI%' ) AS a