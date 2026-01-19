CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_practitioner_id AS SELECT
    prctnr_role_idfn.role_plyr_sk AS practitioner_sk,
    prctnr_role_idfn.registn_jrsd_cd AS registration_code,
    prctnr_role_idfn.id_txt AS id_txt,
    prctnr_role_idfn.vld_fr_ts AS valid_from_date_time,
    prctnr_role_idfn.role_plyr_dw_id,
    prctnr_role_idfn.registn_type_ref_cd AS id_type_code,
    prctnr_role_idfn.src_sys_ref_cd AS source_system_txt,
    prctnr_role_idfn.dw_insrt_ts AS dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.prctnr_role_idfn
  WHERE prctnr_role_idfn.vld_to_ts = '9999-12-31 00:00:00'
;
