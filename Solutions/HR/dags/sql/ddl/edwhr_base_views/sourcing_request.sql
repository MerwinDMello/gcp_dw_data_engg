
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.sourcing_request AS SELECT
    sourcing_request.sourcing_request_sid,
    sourcing_request.valid_from_date,
    sourcing_request.recruitment_requisition_sid,
    sourcing_request.job_board_id,
    sourcing_request.source_request_status_id,
    sourcing_request.job_board_type_id,
    sourcing_request.valid_to_date,
    sourcing_request.posting_date,
    sourcing_request.unposting_date,
    sourcing_request.creation_date,
    sourcing_request.requisition_num,
    sourcing_request.source_system_code,
    sourcing_request.dw_last_update_date_time
  FROM
    {{params.param_hr_core_dataset_name}}.sourcing_request
;