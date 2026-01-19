CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_exam AS SELECT
    ref_rad_exam.rad_exam_mnemonic_cs,
    ref_rad_exam.company_code,
    ref_rad_exam.coid,
    ref_rad_exam.rad_exam_name,
    ref_rad_exam.rad_exam_mnemonic,
    ref_rad_exam.rad_exam_type_code,
    ref_rad_exam.active_ind,
    ref_rad_exam.nomenclature_code,
    ref_rad_exam.rad_exam_cpt_code,
    ref_rad_exam.rad_exam_lab_count,
    ref_rad_exam.radiology_exam_pacs_ind,
    ref_rad_exam.confidential_indicator,
    ref_rad_exam.mammography_indicator,
    ref_rad_exam.rad_exam_result_required_ind,
    ref_rad_exam.rad_exam_result_verified_ind,
    ref_rad_exam.facility_mnemonic_cs,
    ref_rad_exam.network_mnemonic_cs,
    ref_rad_exam.source_system_code,
    ref_rad_exam.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_rad_exam
;
