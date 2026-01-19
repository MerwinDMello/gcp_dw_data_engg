create or replace view `{{ params.param_hr_base_views_dataset_name }}.offer`
AS SELECT
    offer.offer_sid,
    offer.valid_from_date,
    offer.valid_to_date,
    offer.offer_num,
    offer.submission_sid,
    offer.sequence_num,
    offer.offer_name,
    offer.accept_date,
    offer.start_date,
    offer.extend_date,
    offer.last_modified_date,
    offer.last_modified_time,
    offer.capture_date,
    offer.salary_amt,
    offer.sign_on_bonus_amt,
    offer.salary_pay_basis_amt,
    offer.hours_per_day_num,
    offer.source_system_code,
    offer.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.offer;