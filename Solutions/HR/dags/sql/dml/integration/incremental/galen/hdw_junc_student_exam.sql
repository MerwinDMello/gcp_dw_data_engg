BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime; 
SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND) ; 


BEGIN TRANSACTION;
UPDATE {{params.param_hr_core_dataset_name}}.junc_student_exam AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = current_ts FROM {{params.param_hr_stage_dataset_name}}.junc_student_exam_wrk AS stg WHERE tgt.student_sid = stg.student_sid
   AND tgt.exam_id = stg.exam_id
   AND (trim(CAST(coalesce(tgt.exam_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.exam_date, DATE '1900-01-01') as STRING))
   OR coalesce(trim(tgt.source_system_code), 'XX') <> coalesce(trim(stg.source_system_code), 'XX'))
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59") ;
 
 
  INSERT INTO {{params.param_hr_core_dataset_name}}.junc_student_exam (student_sid, exam_id, valid_from_date, valid_to_date, exam_date, source_system_code, dw_last_update_date_time)
    SELECT
        stg.student_sid,
        stg.exam_id,
        current_ts,
        DATETIME("9999-12-31 23:59:59"),
        stg.exam_date,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{params.param_hr_stage_dataset_name}}.junc_student_exam_wrk AS stg
        LEFT OUTER JOIN {{params.param_hr_base_views_dataset_name}}.junc_student_exam AS tgt ON tgt.student_sid = stg.student_sid
         AND tgt.exam_id = stg.exam_id
         AND trim(CAST(coalesce(tgt.exam_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.exam_date, DATE '1900-01-01') as STRING))
         AND coalesce(trim(tgt.source_system_code), 'XX') = coalesce(trim(stg.source_system_code), 'XX')
         AND tgt.valid_to_date =  DATETIME("9999-12-31 23:59:59") 
      WHERE tgt.student_sid IS NULL
  ;

/* Test Unique Primary Index constarint set in Teradata*/
SET DUP_COUNT =(
  select count(*)
  from (
  select Student_SID ,Exam_Id ,Valid_From_Date
  from {{params.param_hr_core_dataset_name}}.junc_student_exam
  group by Student_SID ,Exam_Id ,Valid_From_Date
  having count(*)>1
  )
);
IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE =concat('Duplicates are not allowed in the table :edwhr_copy.junc_student_exam');
ELSE  
  COMMIT  TRANSACTION;
END IF;

END;


