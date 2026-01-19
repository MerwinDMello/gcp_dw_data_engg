create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_sector`
AS SELECT
ref_sector.sector_code,
ref_sector.sector_desc,
ref_sector.source_system_code,
ref_sector.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_sector;