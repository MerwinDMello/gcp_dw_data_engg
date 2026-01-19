create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_disciplinary_indicator`
AS SELECT
ref_disciplinary_indicator.disciplinary_ind,
ref_disciplinary_indicator.disciplinary_desc,
ref_disciplinary_indicator.source_system_code,
ref_disciplinary_indicator.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_disciplinary_indicator;