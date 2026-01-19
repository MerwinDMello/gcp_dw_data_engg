/*==============================================================*/
/* Table: Ref_Calibration_Box                                   */
/*==============================================================*/
/***************************************************************************************
   ******************   E D W H R    R S T C    V I E W   ******************
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_calibration_box AS SELECT
    ref_calibration_box.calibration_box_id,
    ref_calibration_box.calibration_box_desc,
    ref_calibration_box.source_system_code,
    ref_calibration_box.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_calibration_box
;
