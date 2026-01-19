BEGIN
DECLARE current_ts datetime; 
SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND) ; 

TRUNCATE TABLE {{params.param_hr_stage_dataset_name}}.junc_student_exam_wrk;

INSERT INTO {{params.param_hr_stage_dataset_name}}.junc_student_exam_wrk (student_sid, exam_id, valid_from_date, valid_to_date, exam_date, source_system_code, dw_last_update_date_time)
    SELECT
        s.student_sid,
        1 AS exam_id,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.rn_exam_date AS exam_date,
        'C' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{params.param_hr_stage_dataset_name}}.galen_stg AS stg
        INNER JOIN {{params.param_hr_base_views_dataset_name}}.nursing_student AS s ON CAST(stg.student_id AS INT64) = s.student_num
         AND s.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(s.source_system_code) = 'C'
      WHERE stg.rn_exam_date IS NOT NULL
       AND (stg.rn_exam_date) <> CAST('1900-01-01' AS DATE)
      GROUP BY 1, 2, 3, 4, 5, 6, 7
    UNION ALL
    SELECT
        s.student_sid,
        2 AS exam_id,
        current_ts AS valid_from_date,
	       DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.pn_vn_exam_date AS exam_date,
        'C' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{params.param_hr_stage_dataset_name}}.galen_stg AS stg
        INNER JOIN {{params.param_hr_base_views_dataset_name}}.nursing_student AS s ON cast(stg.student_id as INT64) = s.student_num
         AND s.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(s.source_system_code) = 'C'
      WHERE stg.pn_vn_exam_date IS NOT NULL
       AND stg.pn_vn_exam_date <> CAST('1900-01-01' AS DATE)
      GROUP BY 1, 2, 3, 4, 5, 6, 7
  ;
END;