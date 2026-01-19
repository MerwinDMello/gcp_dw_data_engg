create or replace view `{{ params.param_hr_base_views_dataset_name }}.offer_detail`
AS SELECT
    offer_detail.offer_sid,
    offer_detail.element_detail_entity_text,
    offer_detail.element_detail_type_text,
    offer_detail.element_detail_seq_num,
    offer_detail.valid_from_date,
    offer_detail.valid_to_date,
    offer_detail.element_detail_id,
    offer_detail.element_detail_value_text,
    offer_detail.source_system_code,
    offer_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.offer_detail;