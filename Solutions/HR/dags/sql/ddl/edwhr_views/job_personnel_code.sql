/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.job_personnel_code AS SELECT
      a.job_code_sid,
      a.position_sid,
      a.personnel_type_code,
      a.personnel_code,
      a.hr_company_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.required_flag_ind,
      a.personnel_code_time_pct,
      a.proficiency_level_desc,
      a.weight_amt,
      a.subject_code,
      a.job_code,
      a.position_code,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.job_personnel_code AS a
  ;

