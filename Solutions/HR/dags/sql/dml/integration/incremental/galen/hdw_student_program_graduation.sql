BEGIN
DECLARE current_ts DATETIME;
DECLARE DUP_COUNT INT64;
SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND); 
BEGIN TRANSACTION;
DELETE FROM {{ params.param_hr_core_dataset_name }}.student_program_graduation where Source_System_Code='C';
UPDATE {{ params.param_hr_core_dataset_name }}.student_program_graduation AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND,
dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.student_program_graduation_wrk AS stg WHERE tgt.student_sid = stg.student_sid
AND tgt.nursing_program_id = stg.nursing_program_id
AND (trim(CAST(coalesce(tgt.graduation_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.graduation_date, DATE '1900-01-01') as STRING))
OR trim(CAST(coalesce(tgt.cumulative_gpa, -999) as STRING)) <> trim(CAST(coalesce(stg.cumulative_gpa, -999) as STRING))
OR trim(CAST(coalesce(tgt.nursing_school_id, -999) as STRING)) <> trim(CAST(coalesce(stg.nursing_school_id, -999) as STRING))
OR trim(CAST(coalesce(tgt.campus_id, -999) as STRING)) <> trim(CAST(coalesce(stg.campus_id, -999) as STRING))
OR coalesce(trim(tgt.source_system_code), 'xx') <> coalesce(trim(stg.source_system_code), 'xx'))
AND date(tgt.valid_to_date) = DATETIME("9999-12-31");
INSERT INTO {{ params.param_hr_core_dataset_name }}.student_program_graduation (student_sid, nursing_program_id, valid_from_date, graduation_date, cumulative_gpa, nursing_school_id, campus_id, valid_to_date, source_system_code, dw_last_update_date_time)
SELECT
stg.student_sid,
stg.nursing_program_id,
current_ts,
stg.graduation_date,
stg.cumulative_gpa,
stg.nursing_school_id,
stg.campus_id,
DATETIME("9999-12-31 23:59:59"),
stg.source_system_code,
current_ts
FROM
{{ params.param_hr_stage_dataset_name }}.student_program_graduation_wrk AS stg
LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.student_program_graduation AS tgt ON tgt.student_sid = stg.student_sid
AND tgt.nursing_program_id = stg.nursing_program_id
AND trim(CAST(coalesce(tgt.graduation_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.graduation_date, DATE '1900-01-01') as STRING))
AND trim(CAST(coalesce(tgt.cumulative_gpa, -999) as STRING)) = trim(CAST(coalesce(stg.cumulative_gpa, -999) as STRING))
AND trim(CAST(coalesce(tgt.nursing_school_id, -999) as STRING)) = trim(CAST(coalesce(stg.nursing_school_id, -999) as STRING))
AND coalesce(tgt.campus_id, -999) = coalesce(stg.campus_id, -999)
AND coalesce(trim(tgt.source_system_code), 'xx') = coalesce(trim(stg.source_system_code), 'xx')
AND date(tgt.valid_to_date) = DATETIME("9999-12-31")
WHERE tgt.student_sid IS NULL
QUALIFY row_number() OVER (PARTITION BY stg.student_sid, stg.nursing_program_id, stg.valid_from_date ORDER BY stg.campus_id DESC) = 1
;
SET
DUP_COUNT = (
SELECT
COUNT(*)
FROM (
SELECT
Student_SID ,Nursing_Program_Id ,Valid_From_Date
FROM
{{ params.param_hr_core_dataset_name }}.student_program_graduation
GROUP BY
Student_SID ,Nursing_Program_Id ,Valid_From_Date
HAVING
COUNT(*) > 1 ) );
IF
DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.student_program_graduation');
ELSE
COMMIT TRANSACTION;
END IF;
END;