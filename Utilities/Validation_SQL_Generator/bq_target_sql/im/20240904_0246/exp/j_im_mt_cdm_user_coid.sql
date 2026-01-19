-- Translation time: 2024-09-04T09:17:14.094843Z
-- Translation job ID: ac9a27eb-4692-4476-865e-b96823e9df9e
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240904_0246/input/exp/j_im_mt_cdm_user_coid.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(count(*), 0)) AS source_string
  FROM
    (
      SELECT DISTINCT
          encnt_to_role.role_plyr_sk AS role_plyr_sk,
          encnt_to_role.vld_to_ts AS vld_to_ts,
          encnt_to_role.company_code AS company_code,
          encnt_to_role.coid AS coid,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
        FROM
          `hca-hin-dev-cur-comp`.edwim_base_views.encnt_to_role
        WHERE encnt_to_role.vld_to_ts = DATETIME '9999-12-31 00:00:00'
         AND NOT upper(encnt_to_role.rl_type_ref_cd) LIKE 'INSUR%'
         AND NOT upper(encnt_to_role.rl_type_ref_cd) LIKE 'FACIL%'
         AND NOT upper(encnt_to_role.rl_type_ref_cd) LIKE 'PATI%'
    ) AS a
;
