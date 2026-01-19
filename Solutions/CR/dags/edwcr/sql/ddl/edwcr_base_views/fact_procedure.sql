CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.fact_procedure AS SELECT
    a.prcdr_sk AS procedure_sk,
    a.patient_dw_id,
    a.company_code,
    a.coid,
    a.vld_fr_ts AS valid_from_date_time,
    a.vld_to_ts AS valid_to_date_time,
    a.encnt_sk AS encounter_sk,
    a.prty_seq AS priority_sequence,
    a.src_sys_orgnl_cd AS procedure_code,
    a.prcdr_txt AS procedure_desc,
    a.prcdr_ts AS procedure_date_time,
    a.ansth_cd_sk AS anesthesia_sk,
    a.src_sys_ref_cd AS source_system_code,
    a.dw_insrt_ts AS dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.prcdr_dtl AS a
;
