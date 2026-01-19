CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_sub_functional_dept_nurse_spcl AS SELECT
    ref_sub_functional_dept_nurse_spcl.sub_functional_dept_num,
    ref_sub_functional_dept_nurse_spcl.functional_dept_num,
    ref_sub_functional_dept_nurse_spcl.nurse_specialty_desc,
    ref_sub_functional_dept_nurse_spcl.source_system_code,
    ref_sub_functional_dept_nurse_spcl.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_sub_functional_dept_nurse_spcl
;
