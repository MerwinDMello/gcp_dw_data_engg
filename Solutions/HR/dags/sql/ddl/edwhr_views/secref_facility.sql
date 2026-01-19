
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.secref_facility AS SELECT
      secref_facility.company_code,
      secref_facility.user_id,
      secref_facility.co_id
    FROM
      {{ params.param_hr_base_views_dataset_name }}.secref_facility
  ;

