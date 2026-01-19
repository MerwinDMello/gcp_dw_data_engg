create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_offer_status`
AS SELECT
    ref_offer_status.offer_status_id,
    ref_offer_status.offer_status_desc,
    ref_offer_status.source_system_code,
    ref_offer_status.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_offer_status;