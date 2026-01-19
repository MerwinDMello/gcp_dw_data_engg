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
stg.lawson_company_num,
stg.status_type_code,
stg.status_code,
'Y' as active_dw_ind,
'9999-12-31' as eff_to_date,
stg.process_level_code as process_level_code,
stg.security_key_text,
stg.source_system_code,
current_ts dw_last_update_date_time
from
(
select 
pj.company as lawson_company_num,
'REQ' as status_type_code,
trim(pj.req_status) as status_code,
pj.process_level as  process_level_code,
CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(CAST(company AS string)))), TRIM(CAST(company AS string)), '-', '00000', '-', '00000') AS security_key_text,
'L' as source_system_code
from  {{ params.param_hr_stage_dataset_name }}.pajobreq pj
group by 1,2,3,4,5,6

union all

select 
es.company as lawson_company_num,
'EMP' as status_type_code,
trim(es.emp_status) as status_code,
'00000' as process_level_code,
CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(CAST(es.company AS string)))), TRIM(CAST(es.company AS string)), '-', '00000', '-', '00000') AS security_key_text,
'L' as source_system_code
from {{ params.param_hr_stage_dataset_name }}.emstatus es
group by 1,2,3,4,5,6

union all


select 
hus.company as lawson_company_num,
'AUX' as status_type_code,
trim(hus.a_field) as status_code,
'00000' as process_level_code,
CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(CAST(hus.company AS string)))), TRIM(CAST(hus.company AS string)), '-', '00000', '-', '00000') AS security_key_text,
'L' as source_system_code
from   {{ params.param_hr_stage_dataset_name }}.hrempusf hus
where trim(hus.field_key)='88'

group by 1,2,3,4,5,6
 ) stg 

group by 1,2,3,4,5,6,7,8,9

));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.status
where date(valid_to_date) = '9999-12-31' AND source_system_code = 'L'
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




