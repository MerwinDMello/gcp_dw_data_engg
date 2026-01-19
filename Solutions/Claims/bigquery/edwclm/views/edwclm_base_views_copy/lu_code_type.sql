CREATE OR REPLACE VIEW {{ params.param_clm_mirrored_base_views_dataset_name }}.lu_code_type
AS SELECT
    lu_code_type.code_type_id,
    lu_code_type.code_type_desc,
    lu_code_type.dw_last_update_date_time,
    lu_code_type.source_system_code
  FROM
    {{ params.param_mirroring_project_id }}.{{ params.param_clm_mirrored_core_dataset_name }}.lu_code_type
;
