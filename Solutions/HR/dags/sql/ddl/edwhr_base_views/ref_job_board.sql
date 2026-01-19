
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_job_board AS SELECT
    ref_job_board.job_board_id,
    ref_job_board.job_board_type_id,
    ref_job_board.recruitment_source_id,
    ref_job_board.source_system_code,
    ref_job_board.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_job_board
;