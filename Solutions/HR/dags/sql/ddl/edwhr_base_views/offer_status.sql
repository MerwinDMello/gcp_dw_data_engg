create or replace view `{{ params.param_hr_base_views_dataset_name }}.offer_status`
AS SELECT
    offer_status.offer_sid,
    offer_status.valid_from_date,
    offer_status.offer_status_id,
    offer_status.valid_to_date,
    offer_status.source_system_code,
    offer_status.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.offer_status;