-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_BI_Views/ref_hiring_step_status_order.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.ref_hiring_step_status_order AS SELECT
    a.step_status_order_id,
    a.step_name,
    a.submission_status_name,
    a.step_status_order_num,
    a.step_status_name,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.ref_hiring_step_status_order AS a
;
