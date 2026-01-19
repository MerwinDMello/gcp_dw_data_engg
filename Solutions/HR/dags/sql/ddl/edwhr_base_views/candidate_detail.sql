create or replace view `{{ params.param_hr_base_views_dataset_name }}.candidate_detail`
AS SELECT
    candidate_detail.candidate_sid,
    candidate_detail.element_detail_entity_text,
    candidate_detail.element_detail_type_text,
    candidate_detail.element_detail_seq,
    candidate_detail.valid_from_date,
    candidate_detail.valid_to_date,
    candidate_detail.element_detail_id,
    candidate_detail.element_detail_value_text,
    candidate_detail.source_system_code,
    candidate_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.candidate_detail;