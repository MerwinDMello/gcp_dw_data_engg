CREATE OR REPLACE VIEW
  {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla AS
SELECT
  pathway_id,
  microstep_num,
  sla_day_cnt,
  day_type_code,
  source_system_code,
  dw_last_update_date_time
FROM
  {{ params.param_hr_core_dataset_name }}.microstep_pathway_sla;