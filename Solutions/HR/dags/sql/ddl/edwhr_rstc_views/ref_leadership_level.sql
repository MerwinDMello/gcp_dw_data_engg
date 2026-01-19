/*==============================================================*/
/* Table: Ref_Leadership_Level                                  */
/*==============================================================*/
/***************************************************************************************
   ******************   E D W H R    R S T C    V I E W   ******************
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_leadership_level AS SELECT
    ref_leadership_level.leadership_level_sid,
    ref_leadership_level.leadership_level_desc,
    ref_leadership_level.source_system_code,
    ref_leadership_level.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_leadership_level
;
