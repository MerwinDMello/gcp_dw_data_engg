  /*  Truncate Worktable Table     */
BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE
  current_ts datetime;
SET
  current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.employee_performance_review_wrk;
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.employee_performance_review_wrk (employee_sid,
    review_sequence_num,
    valid_from_date,
    valid_to_date,
    reviewer_employee_sid,
    scheduled_review_date,
    review_type_code,
    actual_review_date,
    performance_rating_code,
    last_update_date,
    last_update_time,
    last_updated_3_4_login_code,
    total_score_num,
    review_desc,
    review_schedule_type_code,
    next_review_date,
    next_review_code,
    last_review_date,
    employee_num,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
SELECT
  emp.employee_sid,
  stg.seq_nbr AS review_sequence_num,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  emp1.employee_sid AS reviewer_employee_sid,
  stg.sched_date AS scheduled_review_date,
  TRIM(stg.code) AS review_type_code,
  stg.actual_date AS actual_review_date,
  TRIM(stg.rating) AS performance_rating_code,
  stg.date_stamp AS last_update_date,
  CAST(stg.time_stamp AS TIME) AS last_update_time,
  LEFT(TRIM(stg.user_id),7) AS last_updated_3_4_login_code,
  stg.total_score AS total_score_num,
  TRIM(stg.description) AS review_desc,
  TRIM(stg.rev_schedule) AS review_schedule_type_code,
  stg.next_review AS next_review_date,
  stg.next_rev_code AS next_review_code,
  stg.last_review AS last_review_date,
  CAST(stg.employee AS int64) AS employee_num,
  stg.company AS lawson_company_num,
  '00000' AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.review AS stg
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.employee AS emp
ON
  UPPER(TRIM(CAST(emp.employee_num AS string))) = UPPER(TRIM(stg.employee))
  AND emp.lawson_company_num = stg.company
  AND DATE(emp.valid_to_date) = "9999-12-31"
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.employee AS emp1
ON
  emp1.employee_num = stg.by_employee
  AND emp1.lawson_company_num = stg.company
  AND DATE(emp1.valid_to_date) = "9999-12-31"
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  15,
  16,
  17,
  18,
  19,
  20,
  21,
  22,
  23 ;
BEGIN TRANSACTION;
UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_performance_review AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_performance_review_wrk AS stg
WHERE
  tgt.employee_sid = stg.employee_sid
  AND tgt.review_sequence_num = stg.review_sequence_num
  AND (COALESCE(tgt.reviewer_employee_sid, -999) <> COALESCE(stg.reviewer_employee_sid, -999)
    OR COALESCE(tgt.scheduled_review_date, DATE '1901-01-01') <> COALESCE(stg.scheduled_review_date, DATE '1901-01-01')
    OR UPPER(COALESCE(TRIM(tgt.review_type_code), '')) <> UPPER(COALESCE(TRIM(stg.review_type_code), ''))
    OR COALESCE(tgt.actual_review_date, DATE '1901-01-01') <> COALESCE(stg.actual_review_date, DATE '1901-01-01')
    OR UPPER(COALESCE(TRIM(tgt.performance_rating_code), '')) <> UPPER(COALESCE(TRIM(stg.performance_rating_code), ''))
    OR COALESCE(tgt.last_update_date, DATE '1901-01-01') <> COALESCE(stg.last_update_date, DATE '1901-01-01')
    OR COALESCE(tgt.last_update_time, CAST('' AS time)) <> COALESCE(stg.last_update_time, CAST('' AS time))
    OR UPPER(COALESCE(TRIM(tgt.last_updated_3_4_login_code), '')) <> UPPER(COALESCE(TRIM(stg.last_updated_3_4_login_code), ''))
    OR COALESCE(tgt.total_score_num, -999) <> COALESCE(stg.total_score_num, -999)
    OR UPPER(COALESCE(TRIM(tgt.review_desc), '')) <> UPPER(COALESCE(TRIM(stg.review_desc), ''))
    OR UPPER(COALESCE(TRIM(tgt.review_schedule_type_code), '')) <> UPPER(COALESCE(TRIM(stg.review_schedule_type_code), ''))
    OR COALESCE(tgt.next_review_date, DATE '1901-01-01') <> COALESCE(stg.next_review_date, DATE '1901-01-01')
    OR UPPER(COALESCE(TRIM(tgt.next_review_code), '')) <> UPPER(COALESCE(TRIM(stg.next_review_code), ''))
    OR COALESCE(tgt.last_review_date, DATE '1901-01-01') <> COALESCE(stg.last_review_date, DATE '1901-01-01')
    OR COALESCE(tgt.employee_num, -999) <> COALESCE(stg.employee_num, -999)
    OR COALESCE(tgt.lawson_company_num, -999) <> COALESCE(stg.lawson_company_num, -999)
    OR UPPER(COALESCE(TRIM(tgt.process_level_code), '')) <> UPPER(COALESCE(TRIM(stg.process_level_code), ''))
    OR UPPER(COALESCE(TRIM(tgt.source_system_code), '')) <> UPPER(COALESCE(TRIM(stg.source_system_code), '')))
  AND DATE(tgt.valid_to_date) = "9999-12-31"
  AND tgt.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.employee_performance_review (employee_sid,
    review_sequence_num,
    valid_from_date,
    valid_to_date,
    reviewer_employee_sid,
    scheduled_review_date,
    review_type_code,
    actual_review_date,
    performance_rating_code,
    last_update_date,
    last_update_time,
    last_updated_3_4_login_code,
    total_score_num,
    review_desc,
    review_schedule_type_code,
    next_review_date,
    next_review_code,
    last_review_date,
    employee_num,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.employee_sid,
  stg.review_sequence_num,
  current_ts,
  stg.valid_to_date,
  stg.reviewer_employee_sid,
  stg.scheduled_review_date,
  stg.review_type_code,
  stg.actual_review_date,
  stg.performance_rating_code,
  stg.last_update_date,
  stg.last_update_time,
  stg.last_updated_3_4_login_code,
  stg.total_score_num,
  stg.review_desc,
  stg.review_schedule_type_code,
  stg.next_review_date,
  stg.next_review_code,
  stg.last_review_date,
  stg.employee_num,
  stg.lawson_company_num,
  stg.process_level_code,
  stg.source_system_code,
  current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_performance_review_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.employee_performance_review AS tgt
ON
  tgt.employee_sid = stg.employee_sid
  AND tgt.review_sequence_num = stg.review_sequence_num
  AND COALESCE(tgt.reviewer_employee_sid, -999) = COALESCE(stg.reviewer_employee_sid, -999)
  AND COALESCE(tgt.scheduled_review_date, DATE '1901-01-01') = COALESCE(stg.scheduled_review_date, DATE '1901-01-01')
  AND UPPER(COALESCE(TRIM(tgt.review_type_code), '')) = UPPER(COALESCE(TRIM(stg.review_type_code), ''))
  AND COALESCE(tgt.actual_review_date, DATE '1901-01-01') = COALESCE(stg.actual_review_date, DATE '1901-01-01')
  AND UPPER(COALESCE(TRIM(tgt.performance_rating_code), '')) = UPPER(COALESCE(TRIM(stg.performance_rating_code), ''))
  AND COALESCE(tgt.last_update_date, DATE '1901-01-01') = COALESCE(stg.last_update_date, DATE '1901-01-01')
  AND COALESCE(tgt.last_update_time, CAST('' AS time)) = COALESCE(stg.last_update_time, CAST('' AS time))
  AND UPPER(COALESCE(TRIM(tgt.last_updated_3_4_login_code), '')) = UPPER(COALESCE(TRIM(stg.last_updated_3_4_login_code), ''))
  AND COALESCE(tgt.total_score_num, -999) = COALESCE(stg.total_score_num, -999)
  AND UPPER(COALESCE(TRIM(tgt.review_desc), '')) = UPPER(COALESCE(TRIM(stg.review_desc), ''))
  AND UPPER(COALESCE(TRIM(tgt.review_schedule_type_code), '')) = UPPER(COALESCE(TRIM(stg.review_schedule_type_code), ''))
  AND COALESCE(tgt.next_review_date, DATE '1901-01-01') = COALESCE(stg.next_review_date, DATE '1901-01-01')
  AND UPPER(COALESCE(TRIM(tgt.next_review_code), '')) = UPPER(COALESCE(TRIM(stg.next_review_code), ''))
  AND COALESCE(tgt.last_review_date, DATE '1901-01-01') = COALESCE(stg.last_review_date, DATE '1901-01-01')
  AND COALESCE(tgt.employee_num, -999) = COALESCE(stg.employee_num, -999)
  AND COALESCE(tgt.lawson_company_num, -999) = COALESCE(stg.lawson_company_num, -999)
  AND UPPER(COALESCE(TRIM(tgt.process_level_code), '')) =UPPER( COALESCE(TRIM(stg.process_level_code), ''))
  AND UPPER(COALESCE(TRIM(tgt.source_system_code), '')) = UPPER(COALESCE(TRIM(stg.source_system_code), ''))
  AND DATE(tgt.valid_to_date) = "9999-12-31"
WHERE
  tgt.employee_sid IS NULL ;

UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_performance_review AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts
WHERE
  DATE(tgt.valid_to_date) = "9999-12-31"
  AND (tgt.employee_sid,
    tgt.review_sequence_num) NOT IN(
  SELECT
    DISTINCT AS STRUCT employee_performance_review_wrk.employee_sid,
    employee_performance_review_wrk.review_sequence_num
  FROM
    {{ params.param_hr_stage_dataset_name }}.employee_performance_review_wrk )
  AND tgt.source_system_code = 'L';

SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Employee_SID,
      Review_Sequence_Num,
      Valid_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.employee_performance_review
    GROUP BY
      Employee_SID,
      Review_Sequence_Num,
      Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.hr_company');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;