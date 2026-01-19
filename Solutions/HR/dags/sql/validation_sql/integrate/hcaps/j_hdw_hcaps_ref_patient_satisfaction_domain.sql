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
select
        ref_pat_sat_que_dom_xwalk_stg.domain_id as domain_id,
        ref_pat_sat_que_dom_xwalk_stg.domain_label as domain_desc,
        ref_pat_sat_que_dom_xwalk_stg.domaingroupid as domain_group_id,
        ref_pat_sat_que_dom_xwalk_stg.domaingroupdesc as domain_group_desc,
        'H' as source_system_code,
        current_ts as dw_last_update_date_time
      from
        {{ params.param_hr_stage_dataset_name }}.ref_pat_sat_que_dom_xwalk_stg
      group by 1, 2, 3, 4
      qualify row_number() over (partition by domain_id order by domain_id desc) = 1
              
 ));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.ref_patient_satisfaction_domain
where dw_last_update_date_time >= tableload_start_time  - interval 1 minute);

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




