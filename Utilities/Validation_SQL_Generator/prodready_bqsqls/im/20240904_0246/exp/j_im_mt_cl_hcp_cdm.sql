-- Translation time: 2024-09-04T09:17:14.094843Z
-- Translation job ID: ac9a27eb-4692-4476-865e-b96823e9df9e
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240904_0246/input/exp/j_im_mt_cl_hcp_cdm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT DISTINCT t1.hcp_dw_id AS hcp_dw_id,
                   t3.mt_user_id AS mt_user_id,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
   FROM {{ params.param_im_auth_base_views_dataset_name }}.clinical_health_care_provider AS t1
   INNER JOIN
     (SELECT prctnr_role_idfn.role_plyr_sk,
             substr(trim(prctnr_role_idfn.id_txt), 1, 10) AS npi
      FROM {{ params.param_im_auth_base_views_dataset_name }}.prctnr_role_idfn
      WHERE upper(rtrim(prctnr_role_idfn.registn_type_ref_cd)) = 'NPI'
        AND prctnr_role_idfn.vld_to_ts = DATETIME '9999-12-31 00:00:00'
        AND rtrim(prctnr_role_idfn.id_txt) <> '' ) AS t2 ON t1.national_provider_id = SAFE_CAST(t2.npi AS FLOAT64)
   INNER JOIN
     (SELECT prctnr_role_idfn.role_plyr_sk,
             mt_user_id AS mt_user_id
      FROM {{ params.param_im_auth_base_views_dataset_name }}.prctnr_role_idfn
      CROSS JOIN UNNEST(ARRAY[ substr(prctnr_role_idfn.id_txt, 10, 7) ]) AS mt_user_id
      WHERE upper(rtrim(prctnr_role_idfn.registn_type_ref_cd)) = 'LOGON_ID'
        AND length(trim(mt_user_id)) = 7
        AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(mt_user_id), 4, 4), '[A-Za-z_]') = 0
        AND {{ params.param_im_bqutil_fns_dataset_name }}.cw_regexp_instr_2(substr(trim(mt_user_id), 1, 3), '[0-9_]') = 0 ) AS t3 ON t2.role_plyr_sk = t3.role_plyr_sk
   WHERE t1.national_provider_id IS NOT NULL
     AND t1.national_provider_id > 0 QUALIFY row_number() OVER (PARTITION BY hcp_dw_id
                                                                ORDER BY upper(mt_user_id) DESC) = 1 ) AS a