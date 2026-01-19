/*==============================================================*/
/* Table: Ref_Timeframe                                         */
/*==============================================================*/
/***************************************************************************************
   ******************   E D W H R    R S T C    V I E W   ******************
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_timeframe AS SELECT
    ref_timeframe.timeframe_id,
    ref_timeframe.timeframe_desc,
    ref_timeframe.source_system_code,
    ref_timeframe.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_timeframe
;
