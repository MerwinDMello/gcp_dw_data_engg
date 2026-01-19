/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.recruitment_job_parameter_detail AS SELECT
    recruitment_job_parameter_detail.recruitment_job_parameter_sid,
    recruitment_job_parameter_detail.element_detail_entity_text,
    recruitment_job_parameter_detail.element_detail_type_text,
    recruitment_job_parameter_detail.element_detail_seq,
    recruitment_job_parameter_detail.valid_from_date,
    recruitment_job_parameter_detail.valid_to_date,
    recruitment_job_parameter_detail.job_parameter_num,
    recruitment_job_parameter_detail.element_detail_id,
    recruitment_job_parameter_detail.element_detail_value_text,
    recruitment_job_parameter_detail.source_system_code,
    recruitment_job_parameter_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.recruitment_job_parameter_detail
;