CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_special_program AS SELECT
    dim_special_program.special_program_sk,
    dim_special_program.special_program_code,
    dim_special_program.special_program_desc,
    dim_special_program.source_system_code,
    dim_special_program.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_special_program
;
