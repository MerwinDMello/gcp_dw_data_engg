/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.recruitment_job_parameter_detail AS SELECT
      a.recruitment_job_parameter_sid,
      a.element_detail_entity_text,
      a.element_detail_type_text,
      a.element_detail_seq,
      a.valid_from_date,
      a.valid_to_date,
      a.job_parameter_num,
      a.element_detail_id,
      a.element_detail_value_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.recruitment_job_parameter_detail AS a
  ;

