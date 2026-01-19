CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.candidate_work_history_detail
AS SELECT
    candidate_work_history_detail.candidate_work_history_sid,
    candidate_work_history_detail.element_detail_entity_text,
    candidate_work_history_detail.element_detail_type_text,
    candidate_work_history_detail.element_detail_seq_num,
    candidate_work_history_detail.valid_from_date,
    candidate_work_history_detail.valid_to_date,
    candidate_work_history_detail.element_detail_id,
    candidate_work_history_detail.element_detail_value_text,
    candidate_work_history_detail.source_system_code,
    candidate_work_history_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.candidate_work_history_detail;