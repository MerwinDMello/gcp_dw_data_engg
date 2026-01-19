/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.recruitment_requisition_history AS SELECT
      a.recruitment_requisition_sid,
      a.creation_date_time,
      a.requisition_status_id,
      a.valid_from_date,
      a.valid_to_date,
      a.closed_date_time,
      a.requisition_creator_user_sid,
      a.recruiter_owner_user_sid,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history AS a
  ;

