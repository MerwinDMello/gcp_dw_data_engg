CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_medical_specialty AS SELECT
    ref_medical_specialty.med_spcl_dw_id,
    ref_medical_specialty.med_spcl_src_sys_key,
    ref_medical_specialty.entity_sid,
    ref_medical_specialty.med_spcl_desc,
    ref_medical_specialty.display_order,
    ref_medical_specialty.source_system_code,
    ref_medical_specialty.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_medical_specialty
;
