
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hr_secref_process_level AS SELECT
      hr_secref_process_level.company_code,
      hr_secref_process_level.user_id,
      hr_secref_process_level.lawson_company_num,
      hr_secref_process_level.process_level_code
    FROM
      {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level
  ;

