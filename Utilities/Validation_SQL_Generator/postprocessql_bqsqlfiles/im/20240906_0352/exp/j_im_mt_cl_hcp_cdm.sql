-- Translation time: 2024-09-06T08:53:37.054787Z
-- Translation job ID: 52a90b07-cafd-486a-a732-9cdfe62bb38f
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240906_0352/input/exp/j_im_mt_cl_hcp_cdm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT DISTINCT t1.hcp_dw_id AS hcp_dw_id,
                   t3.mt_user_id AS mt_user_id,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
   FROM `hca-hin-dev-cur-comp`.auth_base_views.clinical_health_care_provider AS t1
   INNER JOIN
     (SELECT prctnr_role_idfn.role_plyr_sk,
             substr(trim(prctnr_role_idfn.id_txt), 1, 10) AS npi
      FROM `hca-hin-dev-cur-comp`.auth_base_views.prctnr_role_idfn
      WHERE upper(rtrim(prctnr_role_idfn.registn_type_ref_cd)) = 'NPI'
        AND prctnr_role_idfn.vld_to_ts = DATETIME '9999-12-31 00:00:00'
        AND rtrim(prctnr_role_idfn.id_txt) <> '' ) AS t2 ON COALESCE(CAST(t1.national_provider_id AS STRING), '') = upper(rtrim(t2.npi))
   INNER JOIN
     (SELECT prctnr_role_idfn.role_plyr_sk,
             mt_user_id AS mt_user_id
      FROM `hca-hin-dev-cur-comp`.auth_base_views.prctnr_role_idfn
      CROSS JOIN UNNEST(ARRAY[ substr(prctnr_role_idfn.id_txt, 10, 7) ]) AS mt_user_id
      WHERE upper(rtrim(prctnr_role_idfn.registn_type_ref_cd)) = 'LOGON_ID'
        AND length(trim(mt_user_id)) = 7
        AND `hca-hin-dev-cur-pub`.bqutil_fns.cw_regexp_instr_2(substr(trim(mt_user_id), 4, 4), '[A-Za-z_]') = 0
        AND `hca-hin-dev-cur-pub`.bqutil_fns.cw_regexp_instr_2(substr(trim(mt_user_id), 1, 3), '[0-9_]') = 0 ) AS t3 ON t2.role_plyr_sk = t3.role_plyr_sk
   WHERE t1.national_provider_id IS NOT NULL
     AND t1.national_provider_id > 0 QUALIFY row_number() OVER (PARTITION BY hcp_dw_id
                                                                ORDER BY upper(mt_user_id) DESC) = 1 ) AS a