-- Translation time: 2024-09-05T15:31:41.992Z
-- Translation job ID: 2a0106aa-7f86-44a1-bb13-2e1417e5dce4
-- Source: gs://eim-comp-cs-datamig-dev-0002/im_bulk_conversion_validation/20240905_1030/input/exp/j_im_mt_cdm_user_mnem.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', coalesce(count(*), 0)) AS source_string
FROM
  (SELECT prctnr_role_idfn.role_plyr_sk AS role_plyr_sk,
          prctnr_role_idfn.vld_to_ts AS vld_to_ts,
          upper(prctnr_role_idfn.id_txt) AS user_mnemonic,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS time_stamp
   FROM {{ params.param_im_base_views_dataset_name }}.prctnr_role_idfn
   WHERE upper(rtrim(prctnr_role_idfn.registn_type_ref_cd)) = 'USER_MNE'
     AND prctnr_role_idfn.vld_to_ts = DATETIME '9999-12-31 00:00:00' QUALIFY row_number() OVER (PARTITION BY role_plyr_sk
                                                                                                ORDER BY prctnr_role_idfn.vld_fr_ts DESC) = 1 ) AS a