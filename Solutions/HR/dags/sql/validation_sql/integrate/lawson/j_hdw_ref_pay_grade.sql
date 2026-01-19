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
 pay_grade as pay_grade_code,
'null' as pay_grade_desc,
source_system_code,
current_ts as dw_last_update_date_time
from (
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code ,1 as priority  from 
{{ params.param_hr_stage_dataset_name }}.employee
union distinct
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code,1 as priority  from 
{{ params.param_hr_stage_dataset_name }}.pajobreq
union distinct
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code ,1 as priority from 
{{ params.param_hr_stage_dataset_name }}.jobcode
union distinct
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code ,1 as priority from 
{{ params.param_hr_stage_dataset_name }}.prsagdtl
union distinct
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code ,1 as priority from 
{{ params.param_hr_stage_dataset_name }}.paposition
union distinct
select distinct(trim(pay_grade)) as pay_grade,'L' as source_system_code,1 as priority  from 
{{ params.param_hr_stage_dataset_name }}.paemppos
where pay_grade not in (select pay_grade_code from {{ params.param_hr_core_dataset_name }}.ref_pay_grade )
and case when pay_grade='' then null else pay_grade end is not null)
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.ref_pay_grade
where dw_last_update_date_time >= tableload_start_time  - interval 1 minute
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




