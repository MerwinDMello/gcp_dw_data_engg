begin

declare  dup_count int64;
DECLARE
  current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

begin transaction;

delete
from {{ params.param_hr_core_dataset_name }}.bi_employee_detail_snapshot where 1 = 1;

-- create metric data 
insert into {{ params.param_hr_core_dataset_name }}.bi_employee_detail_snapshot (
employee_sid,
snapshot_date,
employee_num,
employee_first_name,
employee_last_name,
employee_middle_name,
ethnic_origin_code,
gender_code,
adjusted_hire_date,
birth_date,
acute_experience_start_date,
lawson_company_num,
process_level_code,
source_system_code,
dw_last_update_date_time)

select
employee_sid,
DATE(current_ts) as snapshot_date,
employee_num,
employee_first_name,
employee_last_name,
employee_middle_name,
ethnic_origin_code,
gender_code,
adjusted_hire_date,
birth_date,
acute_experience_start_date,
lawson_company_num,
process_level_code,
source_system_code,
current_ts AS dw_last_update_date_time

from {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail;

/* Test Unique Primary Index constarint set in Teradata*/
SET DUP_COUNT =(
  select count(*)
  from (
  select Employee_SID ,Snapshot_Date
  from {{ params.param_hr_core_dataset_name }}.bi_employee_detail_snapshot
  group by Employee_SID ,Snapshot_Date
  having count(*)>1
  )
);
IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE =concat('Duplicates are not allowed in the table :{{ params.param_hr_core_dataset_name }}.bi_employee_detail_snapshot');
ELSE  
  COMMIT  TRANSACTION;
END IF;
 
end;