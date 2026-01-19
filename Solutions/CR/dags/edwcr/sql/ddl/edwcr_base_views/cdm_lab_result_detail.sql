CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_lab_result_detail
   OPTIONS(description='This view contains lab tests, their results, and range information for a result.')
  AS SELECT
      cdm_lab_result_detail.clinical_finding_sk,
      cdm_lab_result_detail.patient_dw_id,
      cdm_lab_result_detail.company_code,
      cdm_lab_result_detail.coid,
      cdm_lab_result_detail.lab_test_type_ref_code,
      cdm_lab_result_detail.lab_test_type_desc,
      cdm_lab_result_detail.lab_test_id_text,
      cdm_lab_result_detail.lab_test_subid_text,
      cdm_lab_result_detail.lab_test_collect_ts,
      cdm_lab_result_detail.lab_test_specimen_received_ts,
      cdm_lab_result_detail.lab_test_reported_ts,
      cdm_lab_result_detail.lab_test_result_status_ts,
      cdm_lab_result_detail.lab_test_result_status_ref_code,
      cdm_lab_result_detail.lab_test_value_numeric_ind,
      cdm_lab_result_detail.lab_test_value_unit_type_code,
      cdm_lab_result_detail.lab_test_value_text,
      cdm_lab_result_detail.lab_test_value_num,
      cdm_lab_result_detail.ref_range_low_text,
      cdm_lab_result_detail.ref_range_high_text,
      cdm_lab_result_detail.ref_range_text,
      cdm_lab_result_detail.abnormal_flag_name,
      cdm_lab_result_detail.source_system_original_code,
      cdm_lab_result_detail.source_system_code,
      cdm_lab_result_detail.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_lab_result_detail
  ;
