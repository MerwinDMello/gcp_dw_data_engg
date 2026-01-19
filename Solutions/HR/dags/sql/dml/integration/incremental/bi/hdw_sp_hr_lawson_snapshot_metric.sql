begin

declare  dup_count int64;

begin transaction;
delete
from {{ params.param_hr_core_dataset_name }}.fact_hr_metric_snapshot where 1 = 1;

-- create metric data 
insert into {{ params.param_hr_core_dataset_name }}.fact_hr_metric_snapshot(
employee_sid, 
requisition_sid,
position_sid, 
date_id,
analytics_msr_sid,
snapshot_date,
dept_sid,
job_class_sid,
job_code_sid,
location_code,
coid,
company_code,
functional_dept_num,
sub_functional_dept_num,
auxiliary_status_sid,
employee_status_sid,
key_talent_id,
integrated_lob_id,
action_code, 
action_reason_text, 
lawson_company_num,
process_level_code,
work_schedule_code,
recruiter_owner_user_sid,
requisition_approval_date,
employee_num,
metric_numerator_qty,
metric_denominator_qty,
source_system_code, 
dw_last_update_date_time)

select
employee_sid, 
requisition_sid,
position_sid, 
date_id,
analytics_msr_sid,
current_date('US/Central') as snapshot_date,
dept_sid,
job_class_sid,
job_code_sid,
location_code,
coid,
company_code,
functional_dept_num,
sub_functional_dept_num,
auxiliary_status_sid,
employee_status_sid,
key_talent_id,
integrated_lob_id,
action_code, 
action_reason_text, 
lawson_company_num,
process_level_code,
work_schedule_code,
recruiter_owner_user_sid,
requisition_approval_date,
employee_num,
metric_numerator_qty,
metric_denominator_qty,
source_system_code, 
timestamp_trunc( current_datetime('US/Central'), SECOND) AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric;


/* Test Unique Primary Index constarint set in Teradata*/
SET DUP_COUNT =(
  select count(*)
  from (
  select Employee_SID, Requisition_SID ,Position_SID ,Date_Id ,Analytics_Msr_Sid ,Snapshot_Date
  from {{ params.param_hr_core_dataset_name }}.fact_hr_metric_snapshot
  group by Employee_SID, Requisition_SID ,Position_SID ,Date_Id ,Analytics_Msr_Sid ,Snapshot_Date
  having count(*)>1
  )
);
IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE =concat('Duplicates are not allowed in the table :{{ params.param_hr_core_dataset_name }}.fact_hr_metric_snapshot');
ELSE  
  COMMIT  TRANSACTION;
END IF;

end;