CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.junc_recruitment_job_board
AS SELECT
    junc_recruitment_job_board.recruitment_job_sid,
    junc_recruitment_job_board.job_board_id,
    junc_recruitment_job_board.valid_from_date,
    junc_recruitment_job_board.posting_board_type_id,
    junc_recruitment_job_board.posting_status_id,
    junc_recruitment_job_board.valid_to_date,
    junc_recruitment_job_board.posting_date,
    junc_recruitment_job_board.unposting_date,
    junc_recruitment_job_board.source_system_code,
    junc_recruitment_job_board.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.junc_recruitment_job_board;