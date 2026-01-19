BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE current_dt DATETIME;
SET current_dt=DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND);
BEGIN TRANSACTION;
UPDATE
  {{ params.param_hr_core_dataset_name }}.candidate_onboarding_event AS tgt
SET
  valid_to_date = current_dt - INTERVAL 1 SECOND,
  dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_wrk AS wrk
WHERE
  wrk.candidate_onboarding_event_sid = tgt.candidate_onboarding_event_sid
  AND (COALESCE(TRIM(wrk.event_type_id), 'XXX') <> COALESCE(TRIM(tgt.event_type_id), 'XXX')
    OR COALESCE(TRIM(wrk.recruitment_requisition_num_text), 'XXX') <> COALESCE(TRIM(tgt.recruitment_requisition_num_text), 'XXX')
    OR COALESCE(wrk.completed_date, '9999-12-30') <> COALESCE(tgt.completed_date, '9999-12-30')
    OR COALESCE(wrk.candidate_sid, 123456) <> COALESCE(tgt.candidate_sid, 123456)
    OR COALESCE(wrk.resource_screening_package_num, 123456) <> COALESCE(tgt.resource_screening_package_num, 123456)
    OR COALESCE(wrk.sequence_num, 123456) <> COALESCE(tgt.sequence_num, 123456)
    OR COALESCE(TRIM(wrk.source_system_code), 'XXX') <> COALESCE(TRIM(tgt.source_system_code), 'XXX'))
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.candidate_onboarding_event (candidate_onboarding_event_sid,
    valid_from_date,
    event_type_id,
    recruitment_requisition_num_text,
    valid_to_date,
    completed_date,
    candidate_sid,
    resource_screening_package_num,
    sequence_num,
    source_system_code,
    dw_last_update_date_time)
SELECT
  wrk.candidate_onboarding_event_sid,
  current_dt AS valid_from_date,
  wrk.event_type_id,
  wrk.recruitment_requisition_num_text,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  wrk.completed_date,
  wrk.candidate_sid,
  wrk.resource_screening_package_num,
  wrk.sequence_num,
  wrk.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_wrk AS wrk
WHERE
  (COALESCE(wrk.candidate_onboarding_event_sid, 123456),
    COALESCE(TRIM(wrk.event_type_id), 'XXX'),
    COALESCE(TRIM(wrk.recruitment_requisition_num_text), 'XXX'),
    COALESCE(wrk.completed_date, '9999-12-30'),
    COALESCE(wrk.candidate_sid, 123456),
    COALESCE(wrk.resource_screening_package_num, 123456),
    COALESCE(wrk.sequence_num, 123456),
    COALESCE(TRIM(wrk.source_system_code), 'XXX')) NOT IN(
  SELECT
    AS STRUCT COALESCE(CAST(tgt.candidate_onboarding_event_sid AS INT64), 123456),
    COALESCE(TRIM(tgt.event_type_id), 'XXX'),
    COALESCE(TRIM(tgt.recruitment_requisition_num_text), 'XXX'),
    COALESCE(tgt.completed_date, '9999-12-30'),
    COALESCE(tgt.candidate_sid, 123456),
    COALESCE(tgt.resource_screening_package_num, 123456),
    COALESCE(tgt.sequence_num, 123456),
    COALESCE(TRIM(tgt.source_system_code), 'XXX')
  FROM
    {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event AS tgt
  WHERE
    tgt.valid_to_date = DATETIME("9999-12-31 23:59:59") ) QUALIFY ROW_NUMBER() OVER (PARTITION BY wrk.candidate_onboarding_event_sid, valid_from_date ORDER BY wrk.candidate_onboarding_event_sid, valid_from_date DESC) = 1 ;
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      candidate_onboarding_event_sid, valid_from_date
    FROM
      {{ params.param_hr_core_dataset_name }}.candidate_onboarding_event
    GROUP BY
      candidate_onboarding_event_sid, valid_from_date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: ' , '{{ params.param_hr_core_dataset_name }}' , '.candidate_onboarding_event');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;