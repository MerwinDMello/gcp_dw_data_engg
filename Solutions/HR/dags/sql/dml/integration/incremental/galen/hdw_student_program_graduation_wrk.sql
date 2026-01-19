BEGIN
DECLARE current_ts datetime;
--DECLARE DUP_COUNT INT64;
SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND) ;

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.student_program_graduation_wrk;
/* Load Work Table */
INSERT INTO {{ params.param_hr_stage_dataset_name }}.student_program_graduation_wrk (student_sid, nursing_program_id, valid_from_date, graduation_date, cumulative_gpa, nursing_school_id, campus_id, valid_to_date, source_system_code, dw_last_update_date_time)
SELECT
s.student_sid ,
p.nursing_program_id,
current_ts AS valid_from_date,
stg.grad_date AS graduation_date,
round(cast(stg.cumulative_gpa as numeric), 2, "ROUND_HALF_EVEN")  AS cumulative_gpa,
1 AS nursing_school_id,
c.campus_id,
DATETIME("9999-12-31 23:59:59") AS valid_to_date,
'C' AS source_system_code,
current_ts AS dw_last_update_date_time
FROM
{{ params.param_hr_stage_dataset_name }}.galen_stg AS stg
INNER JOIN {{ params.param_hr_base_views_dataset_name }}.nursing_student AS s ON cast(stg.student_id as int64) = s.student_num
AND (s.valid_to_date) = DATETIME("9999-12-31 23:59:59")
AND upper(s.source_system_code) = 'C'
INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_program AS p ON stg.program_ver_desc = p.program_name
AND upper(p.source_system_code) = 'C'
LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_school_campus AS c ON c.campus_name = stg.campus_desc
AND upper(c.source_system_code) = 'C'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
;
end;