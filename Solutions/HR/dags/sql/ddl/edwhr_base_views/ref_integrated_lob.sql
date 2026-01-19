create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob`
AS SELECT
  ref_integrated_lob.integrated_lob_id,
  ref_integrated_lob.category_desc,
  ref_integrated_lob.sub_category_desc,
  ref_integrated_lob.process_level_code,
  ref_integrated_lob.dept_code,
  ref_integrated_lob.lob_code,
  ref_integrated_lob.sub_lob_code,
  ref_integrated_lob.functional_dept_desc,
  ref_integrated_lob.sub_functional_dept_desc,
  ref_integrated_lob.match_level_num,
  ref_integrated_lob.match_level_desc,
  ref_integrated_lob.source_system_code,
  ref_integrated_lob.dw_last_update_date_time
FROM
 {{ params.param_hr_core_dataset_name }}.ref_integrated_lob;