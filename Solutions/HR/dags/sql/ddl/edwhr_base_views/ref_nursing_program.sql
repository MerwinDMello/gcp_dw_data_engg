CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_nursing_program AS SELECT
    ref_nursing_program.nursing_program_id,
    ref_nursing_program.program_name,
    ref_nursing_program.program_type_code,
    ref_nursing_program.program_degree_text,
    ref_nursing_program.source_system_code,
    ref_nursing_program.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_nursing_program
;