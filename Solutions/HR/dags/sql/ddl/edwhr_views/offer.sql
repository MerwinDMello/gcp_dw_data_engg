/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.offer AS SELECT
      a.offer_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.offer_num,
      a.submission_sid,
      a.sequence_num,
      a.offer_name,
      a.accept_date,
      a.start_date,
      a.extend_date,
      a.last_modified_date,
      a.last_modified_time,
      a.capture_date,
      a.salary_amt,
      a.sign_on_bonus_amt,
      a.salary_pay_basis_amt,
      a.hours_per_day_num,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.offer AS a
  ;

