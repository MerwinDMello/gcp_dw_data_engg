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
tgttablename = 'edwhr.recruitment_requisition';
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
  {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.recruitment_requisition AS tgt for system_time as of timestamp(tableload_start_time,'US/Central')
ON
  stg.recruitment_requisition_sid = tgt.recruitment_requisition_sid
  AND COALESCE(tgt.requisition_num, -999) = COALESCE(stg.requisition_num, -999)
  AND COALESCE(tgt.lawson_requisition_sid, -9) = COALESCE(stg.lawson_requisition_sid, -9)
  AND COALESCE(tgt.lawson_requisition_num, -9) = COALESCE(stg.lawson_requisition_num, -9)
  AND COALESCE(tgt.hiring_manager_user_sid, -9) = COALESCE(stg.hiring_manager_user_sid, -9)
  AND COALESCE(CAST(tgt.recruitment_requisition_num_text AS STRING), '-999') = COALESCE(CAST(tgt.recruitment_requisition_num_text AS STRING), '-999')
  AND TRIM(CAST(COALESCE(tgt.process_level_code, '-9') AS STRING)) = TRIM(CAST(COALESCE(stg.process_level_code, '-9') AS STRING))
  AND TRIM(CAST(COALESCE(tgt.approved_sw, -9) AS STRING)) = TRIM(CAST(COALESCE(stg.approved_sw, -9) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.target_start_date, DATE '1900-01-01') AS STRING)) = TRIM(CAST(COALESCE(stg.target_start_date, DATE '1900-01-01') AS STRING))
  AND TRIM(CAST(COALESCE(tgt.required_asset_num, -999) AS STRING)) = TRIM(CAST(COALESCE(stg.required_asset_num, -999) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.required_asset_sw, -9) AS STRING)) = TRIM(CAST(COALESCE(stg.required_asset_sw, -9) AS STRING))
  AND COALESCE(tgt.workflow_id, -999) = COALESCE(stg.workflow_id, -999)
  AND COALESCE(tgt.recruitment_job_sid, -9) = COALESCE(stg.recruitment_job_sid, -9)
  AND COALESCE(tgt.job_template_sid, -9) = COALESCE(stg.job_template_sid, -9)
  AND COALESCE(TRIM(tgt.requisition_new_graduate_flag),'') =COALESCE(TRIM(stg.requisition_new_graduate_flag),'')
  AND COALESCE(tgt.lawson_company_num, -9) = COALESCE(stg.lawson_company_num, -9)
  AND COALESCE(TRIM(tgt.source_system_code), '') = COALESCE(TRIM(stg.source_system_code), '')
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.recruitment_requisition_sid IS NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY stg.recruitment_requisition_sid, stg.valid_from_date ORDER BY stg.recruitment_requisition_sid) = 1 


));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition
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




