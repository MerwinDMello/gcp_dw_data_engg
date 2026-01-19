/*==============================================================*/
/* Table: Ref_Performance_Category                              */
/*==============================================================*/
/***************************************************************************************
   ******************   E D W H R    R S T C    V I E W   ******************
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_performance_category AS SELECT
    ref_performance_category.performance_category_id,
    ref_performance_category.performance_category_desc,
    ref_performance_category.source_system_code,
    ref_performance_category.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_performance_category
;
