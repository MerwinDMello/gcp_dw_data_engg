/*edwhr_base_views.ref_coid_hospital_category*/
/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_coid_hospital_category AS SELECT
    ref_coid_hospital_category.coid,
    ref_coid_hospital_category.company_code,
    ref_coid_hospital_category.year_num,
    ref_coid_hospital_category.hospital_level_code,
    ref_coid_hospital_category.prev_year_change_ind,
    ref_coid_hospital_category.manual_hold_ind,
    ref_coid_hospital_category.quartile_rank_num,
    ref_coid_hospital_category.total_rank_num,
    ref_coid_hospital_category.source_system_code,
    ref_coid_hospital_category.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_coid_hospital_category
;