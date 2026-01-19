##########################
## Variable Declaration ##
##########################

BEGIN
DECLARE
tolerance_percent,difference,srctableid,src_rec_count,tgt_rec_count int64;
declare
sourcesysnm,srctablename,tgttablename,audit_type,tableload_run_time,job_name,audit_status string;
declare
tableload_start_time,tableload_end_time,audit_time,current_ts datetime;
SET
current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
SET 
srctableid = Null;
SET
sourcesysnm = @p_source;
SET
srctablename = Null;
SET
tgttablename = concat('edwhr.',@p_table);
SET
audit_type ='VALIDATION_COUNT';
SET
tableload_start_time = @p_tableload_start_time;
SET
tableload_end_time = @p_tableload_end_time;
SET
tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET
job_name = @p_job_name;
SET
audit_time = current_ts;
SET
tolerance_percent = 0;
SET
src_rec_count = 
(select count(*)
from 
(
SELECT DISTINCT
        hpr.surv_id AS respondent_id,
        hpr.recdate AS survey_receive_date,
        'P' AS respondent_type_code,
        sq.survey_sid,
        'H' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hca_pat_resp AS hpr
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.survey_question AS sq ON sq.question_id = hpr.question_id
         AND upper(sq.source_system_code) = 'H'
        INNER JOIN 
        {{ params.param_hr_base_views_dataset_name }}.ref_survey AS rs ON rs.survey_sid = sq.survey_sid
         AND rs.eff_to_date = sq.eff_to_date
      WHERE upper(rs.survey_group_text) = 'PATIENT_SATISFACTION'
       AND rs.eff_to_date = '9999-12-31'
       AND concat(cast(hpr.surv_id as string), cast(hpr.recdate  as string), cast(sq.survey_sid as string)) NOT IN(
        SELECT 
            concat(cast(respondent_id as string),
            cast(survey_receive_date as string),
            cast(survey_sid as string))
          FROM
            {{ params.param_hr_core_dataset_name }}.respondent_detail for system_time as of timestamp(tableload_start_time,'US/Central')
      )

 ));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.respondent_detail
where source_system_code = 'H' and dw_last_update_date_time >= tableload_start_time  - interval 1 minute);

SET
difference = CASE 
              WHEN src_rec_count <> 0 Then CAST(((ABS(tgt_rec_count - src_rec_count)/src_rec_count) * 100) AS INT64)
              WHEN src_rec_count =0 and tgt_rec_count = 0 Then 0
              ELSE tgt_rec_count
              END;

SET
audit_status = CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
END;

##Insert statement
INSERT INTO
 {{ params.param_hr_audit_dataset_name }}.audit_control
VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type, src_rec_count, tgt_rec_count, 
  cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time,
   job_name, audit_time, audit_status );
END; 




