CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.junc_hcp_specialty AS SELECT
    junc_hcp_specialty.hcp_dw_id,
    junc_hcp_specialty.med_spcl_dw_id,
    junc_hcp_specialty.spcl_type_sid,
    junc_hcp_specialty.hcp_spcl_src_sys_key,
    junc_hcp_specialty.entity_sid,
    junc_hcp_specialty.company_code,
    junc_hcp_specialty.coid,
    junc_hcp_specialty.spcl_cert_stts_sid,
    junc_hcp_specialty.taxonomy_sid,
    junc_hcp_specialty.web_publish_stts_code,
    junc_hcp_specialty.spcl_active_ind,
    junc_hcp_specialty.primary_spcl_ind,
    junc_hcp_specialty.source_system_code,
    junc_hcp_specialty.dw_last_update_date_time,
    junc_hcp_specialty.active_dw_ind
  FROM
    {{ params.param_auth_base_views_dataset_name }}.junc_hcp_specialty
;
