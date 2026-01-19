/*==============================================================*/
/* Table: Ref_Location_Willingness                              */
/*==============================================================*/
/***************************************************************************************
   ******************   E D W H R    R S T C    V I E W   ******************
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_location_willingness AS SELECT
    ref_location_willingness.location_willingness_id,
    ref_location_willingness.location_willingness_desc,
    ref_location_willingness.source_system_code,
    ref_location_willingness.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_location_willingness
;
