BEGIN
DECLARE
  DUP_COUNT INT64;

  DECLARE current_dt DATETIME;
  SET current_dt=DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND);

BEGIN TRANSACTION;
UPDATE
  {{ params.param_hr_core_dataset_name }}.candidate_onboarding AS tgt
SET
  valid_to_date = current_dt - INTERVAL 1 SECOND,
  dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_wrk AS wrk
WHERE
  wrk.candidate_onboarding_sid = tgt.candidate_onboarding_sid
  AND (COALESCE(wrk.requisition_sid, 123456 ) <> COALESCE(tgt.requisition_sid, 123456)
    OR COALESCE(wrk.employee_sid, 123456) <> COALESCE(tgt.employee_sid, 123456)
    OR COALESCE(wrk.candidate_sid, 123456) <> COALESCE(tgt.candidate_sid, 123456)
    OR COALESCE(TRIM(wrk.candidate_first_name), 'XXX') <> COALESCE(TRIM(tgt.candidate_first_name), 'XXX')
    OR COALESCE(TRIM(wrk.candidate_last_name), 'XXX') <> COALESCE(TRIM(tgt.candidate_last_name), 'XXX')
    OR COALESCE(wrk.tour_start_date, '9999-12-30') <> COALESCE(tgt.tour_start_date, '9999-12-30')
    OR COALESCE(wrk.tour_id, 123456) <> COALESCE(tgt.tour_id, 123456)
    OR COALESCE(wrk.tour_status_id, 123456) <> COALESCE(tgt.tour_status_id, 123456)
    OR COALESCE(wrk.tour_completion_pct, 123456) <> COALESCE(tgt.tour_completion_pct, 123456)
    OR COALESCE(wrk.workflow_id, 123456) <> COALESCE(tgt.workflow_id, 123456)
    OR COALESCE(wrk.workflow_status_id, 123456) <> COALESCE(tgt.workflow_status_id, 123456)
    OR COALESCE(wrk.email_sent_status_id, 123456) <> COALESCE(tgt.email_sent_status_id, 123456)
    OR COALESCE(wrk.onboarding_confirmation_date, '9999-12-30') <> COALESCE(tgt.onboarding_confirmation_date, '9999-12-30')
    OR COALESCE(TRIM(wrk.recruitment_requisition_num_text), 'XXX') <> COALESCE(TRIM(tgt.recruitment_requisition_num_text), 'XXX')
    OR COALESCE(TRIM(wrk.process_level_code), 'XXX') <> COALESCE(TRIM(tgt.process_level_code), 'XXX')
    OR COALESCE(wrk.applicant_num, 123456) <> COALESCE(tgt.applicant_num, 123456)
    OR COALESCE(TRIM(wrk.source_system_code), 'XXX') <> COALESCE(TRIM(tgt.source_system_code), 'XXX'))
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.candidate_onboarding (candidate_onboarding_sid,
    valid_from_date,
    valid_to_date,
    requisition_sid,
    employee_sid,
    candidate_sid,
    candidate_first_name,
    candidate_last_name,
    tour_start_date,
    tour_id,
    tour_status_id,
    tour_completion_pct,
    workflow_id,
    workflow_status_id,
    email_sent_status_id,
    onboarding_confirmation_date,
    recruitment_requisition_num_text,
    process_level_code,
    applicant_num,
    source_system_code,
    dw_last_update_date_time)
SELECT
  wrk.candidate_onboarding_sid,
  current_dt AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  wrk.requisition_sid,
  wrk.employee_sid,
  wrk.candidate_sid,
  wrk.candidate_first_name,
  wrk.candidate_last_name,
  wrk.tour_start_date,
  wrk.tour_id,
  wrk.tour_status_id,
  wrk.tour_completion_pct,
  wrk.workflow_id,
  wrk.workflow_status_id,
  wrk.email_sent_status_id,
  wrk.onboarding_confirmation_date,
  wrk.recruitment_requisition_num_text,
  wrk.process_level_code,
  wrk.applicant_num,
  wrk.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_wrk AS wrk
WHERE
  (COALESCE(wrk.candidate_onboarding_sid, 123456),
    COALESCE(wrk.requisition_sid, 123456),
    COALESCE(wrk.employee_sid, 123456),
    COALESCE(wrk.candidate_sid, 123456),
    COALESCE(TRIM(wrk.candidate_first_name), 'XXX'),
    COALESCE(TRIM(wrk.candidate_last_name), 'XXX'),
    COALESCE(wrk.tour_start_date, '9999-12-30'),
    COALESCE(wrk.tour_id, 123456),
    COALESCE(wrk.tour_status_id, 123456),
    COALESCE(wrk.tour_completion_pct, 123456),
    COALESCE(wrk.workflow_id, 123456),
    COALESCE(wrk.workflow_status_id, 123456),
    COALESCE(wrk.email_sent_status_id, 123456),
    COALESCE(wrk.onboarding_confirmation_date, '9999-12-30'),
    COALESCE(TRIM(wrk.recruitment_requisition_num_text), 'XXX'),
    COALESCE(TRIM(wrk.process_level_code), 'XXX'),
    COALESCE(wrk.applicant_num, 123456),
    COALESCE(TRIM(wrk.source_system_code), 'XXX')) NOT IN(
  SELECT
    AS STRUCT COALESCE(tgt.candidate_onboarding_sid, 123456),
    COALESCE(tgt.requisition_sid, 123456),
    COALESCE(tgt.employee_sid, 123456),
    COALESCE(tgt.candidate_sid, 123456),
    COALESCE(TRIM(tgt.candidate_first_name), 'XXX'),
    COALESCE(TRIM(tgt.candidate_last_name), 'XXX'),
    COALESCE(tgt.tour_start_date, '9999-12-30'),
    COALESCE(tgt.tour_id, 123456),
    COALESCE(tgt.tour_status_id, 123456),
    COALESCE(tgt.tour_completion_pct, 123456),
    COALESCE(tgt.workflow_id, 123456),
    COALESCE(tgt.workflow_status_id, 123456),
    COALESCE(tgt.email_sent_status_id, 123456),
    COALESCE(tgt.onboarding_confirmation_date, '9999-12-30'),
    COALESCE(tgt.recruitment_requisition_num_text, 'XXX'),
    COALESCE(TRIM(tgt.process_level_code), 'XXX'),
    COALESCE(tgt.applicant_num, 123456),
    COALESCE(TRIM(tgt.source_system_code), 'XXX')
  FROM
    {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding AS tgt
  WHERE
    tgt.valid_to_date = DATETIME("9999-12-31 23:59:59") ) ; /* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT COUNT(*) FROM (
    SELECT candidate_onboarding_sid, valid_from_date
    FROM {{ params.param_hr_core_dataset_name }}.candidate_onboarding
    GROUP BY candidate_onboarding_sid, valid_from_date
    HAVING COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ' , '{{ params.param_hr_core_dataset_name }}' , '.candidate_onboarding');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;