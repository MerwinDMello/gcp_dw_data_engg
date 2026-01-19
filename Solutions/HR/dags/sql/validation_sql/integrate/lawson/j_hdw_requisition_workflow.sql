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
tolerance_percent = 5;
SET
src_rec_count = 
(select count(*)
from 
(
select  
  *
FROM
        {{ params.param_hr_stage_dataset_name }}.requisition_workflow_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.requisition_workflow AS tgt for system_time as of timestamp(tableload_start_time,'US/Central')  
		 ON tgt.requisition_sid = stg.requisition_sid
         AND trim(cast(tgt.workflow_seq_num as string)) = trim(cast(stg.workflow_seq_num as string))
         AND upper(trim(CAST(coalesce(tgt.workflow_workunit_num_text, '0') as STRING))) = upper(trim(CAST(coalesce(stg.workflow_workunit_num_text, '0') as STRING)))
         AND trim(CAST(coalesce(tgt.workflow_activity_num, 0) as STRING)) = trim(CAST(coalesce(stg.workflow_activity_num, 0) as STRING))
         AND upper(trim(coalesce(tgt.workflow_role_name, 'XX'))) = upper(trim(coalesce(stg.workflow_role_name, 'XX')))
         AND upper(trim(coalesce(tgt.workflow_task_name, 'XX'))) = upper(trim(coalesce(stg.workflow_task_name, 'XX')))
         AND trim(CAST(coalesce(tgt.start_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.start_date, DATE '1900-01-01') as STRING))
         AND coalesce(tgt.start_time, TIME '00:00:00') = coalesce(stg.start_time, TIME '00:00:00')
         AND trim(CAST(coalesce(tgt.end_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.end_date, DATE '1900-01-01') as STRING))
         AND coalesce(tgt.end_time, TIME '00:00:00') = coalesce(stg.end_time, TIME '00:00:00')
         AND trim(CAST(coalesce(tgt.workflow_user_id_code, '0') as STRING)) = trim(CAST(coalesce(stg.workflow_user_id_code, '0') as STRING))
         AND trim(CAST(coalesce(tgt.lawson_company_num, 0) as STRING)) = trim(CAST(coalesce(stg.lawson_company_num, 0) as STRING))
         AND upper(trim(coalesce(tgt.process_level_code, 'XX'))) = upper(trim(coalesce(stg.process_level_code, 'XX')))
         AND upper(trim(coalesce(tgt.active_dw_ind, 'X'))) = upper(trim(coalesce(stg.active_dw_ind, 'X')))
         AND tgt.valid_to_date = DATETIME ('9999-12-31 23:59:59')
      WHERE tgt.requisition_sid IS NULL
      QUALIFY row_number() OVER (PARTITION BY stg.requisition_sid, stg.workflow_seq_num, stg.valid_from_date ORDER BY stg.requisition_sid, stg.workflow_seq_num, stg.valid_from_date DESC) = 1
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.requisition_workflow
where date(valid_to_date) = '9999-12-31' and dw_last_update_date_time >= tableload_start_time  - interval 1 minute
) ;

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




