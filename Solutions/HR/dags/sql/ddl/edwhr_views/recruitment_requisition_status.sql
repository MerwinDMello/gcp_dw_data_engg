/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.recruitment_requisition_status AS SELECT
      a.recruitment_requisition_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.requisition_status_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_status AS a
  ;

