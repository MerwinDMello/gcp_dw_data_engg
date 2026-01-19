CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_posting_board_type AS SELECT
    ref_posting_board_type.posting_board_type_id,
    ref_posting_board_type.posting_board_type_code,
    ref_posting_board_type.source_system_code,
    ref_posting_board_type.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_posting_board_type
;