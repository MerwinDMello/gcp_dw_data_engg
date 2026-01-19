-- Translation time: 2024-09-04T09:17:14.094843Z
-- Translation job ID: ac9a27eb-4692-4476-865e-b96823e9df9e
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240904_0246/input/exp/j_im_mt_cdm_user_3_4.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(count(*), 0)) AS source_string
  FROM
    (
      SELECT
          prctnr_role_idfn.role_plyr_sk AS role_plyr_sk,
          prctnr_role_idfn.vld_to_ts AS vld_to_ts,
          mt_user_id AS mt_user_id,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
        FROM
          `hca-hin-dev-cur-comp`.edwim_base_views.prctnr_role_idfn
          CROSS JOIN UNNEST(ARRAY[
            substr(prctnr_role_idfn.id_txt, 10, 7)
          ]) AS mt_user_id
        WHERE upper(rtrim(prctnr_role_idfn.registn_type_ref_cd)) = 'LOGON_ID'
         AND prctnr_role_idfn.vld_to_ts = DATETIME '9999-12-31 00:00:00'
         AND bqutil.fn.cw_regexp_instr_2(substr(mt_user_id, 4, 4), '[A-Za-z_]') = 0
         AND bqutil.fn.cw_regexp_instr_2(substr(mt_user_id, 1, 3), '[0-9_]') = 0
        QUALIFY row_number() OVER (PARTITION BY role_plyr_sk ORDER BY prctnr_role_idfn.vld_fr_ts DESC) = 1
    ) AS a
;
