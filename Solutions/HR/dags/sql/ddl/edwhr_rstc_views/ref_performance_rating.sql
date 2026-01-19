/*==============================================================*/
/* Table: Ref_Performance_Rating                                */
/*==============================================================*/
/***************************************************************************************
   ******************   E D W H R    R S T C    V I E W   ******************
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_performance_rating AS SELECT
    ref_performance_rating.performance_rating_id,
    ref_performance_rating.performance_rating_desc,
    ref_performance_rating.dw_last_update_date_time,
    ref_performance_rating.source_system_code
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_performance_rating
;
