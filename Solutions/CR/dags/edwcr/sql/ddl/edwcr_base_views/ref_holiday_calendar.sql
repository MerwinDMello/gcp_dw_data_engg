CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_holiday_calendar
   OPTIONS(description='Reference holding data for holiday calendar from discover database')
  AS SELECT
      ref_holiday_calendar.holiday_calendar_id,
      ref_holiday_calendar.holiday_calendar_date,
      ref_holiday_calendar.holiday_calendar_desc,
      ref_holiday_calendar.created_date_time,
      ref_holiday_calendar.created_by_3_4_id,
      ref_holiday_calendar.source_last_update_date_time,
      ref_holiday_calendar.updated_by_3_4_id,
      ref_holiday_calendar.active_ind,
      ref_holiday_calendar.source_system_code,
      ref_holiday_calendar.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_holiday_calendar
  ;
