CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.fact_imaging_order AS SELECT
    ord.clnc_ordr_sk AS clinical_order_sk,
    ord.ordr_type_cd AS order_type_sk,  -- was originally named ordr_type_cd_sk
    en.patient_sk AS patient_sk,
    en.encounter_sk AS encounter_sk,
    ord.vld_fr_ts AS valid_from_date_time,
    ord.patient_dw_id AS patient_dw_id,
    ord.company_code AS company_code,
    ord.coid AS coid,
    ord.plcr_splmt_srvc_info_txt AS placer_splmt_serv_info_txt,
    ord.ordr_mnem AS order_mnemonic,
    ord.ordr_ts AS order_date_time,
    ord.ordr_src_ref_cd AS order_source_code,
    ord.ordr_actn_sts_ref_cd AS order_action_status_code,
    ord.ordr_type_ref_cd AS order_type_code,
    ord.ordr_prty_ref_cd AS order_priority_code,
    ord.accsn_txt AS accession_txt,
    ord.effv_fr_dt AS effective_from_date,
    ord.src_sys_orgnl_cd AS source_system_original_code,
    ord.src_sys_ref_cd AS source_system_txt,
    ord.dw_insrt_ts AS dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.clnc_ordr_dtl AS ord
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.fact_encounter AS en ON ord.patient_dw_id = en.patient_dw_id
  WHERE ord.ordr_type_ref_cd LIKE 'rad%'
   AND ord.vld_to_ts = '9999-12-31 00:00:00'
;
