CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_imaging_result_detail
   OPTIONS(description='Contains the imaging result')
  AS SELECT
      cdm_imaging_result_detail.clinical_finding_sk,
      cdm_imaging_result_detail.patient_dw_id,
      cdm_imaging_result_detail.coid,
      cdm_imaging_result_detail.company_code,
      cdm_imaging_result_detail.image_occurence_ts,
      cdm_imaging_result_detail.source_system_original_code,
      cdm_imaging_result_detail.imaging_type_code,
      cdm_imaging_result_detail.procedure_mnemonic_cs,
      cdm_imaging_result_detail.rad_exam_name,
      cdm_imaging_result_detail.source_system_code,
      cdm_imaging_result_detail.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_imaging_result_detail
  ;
