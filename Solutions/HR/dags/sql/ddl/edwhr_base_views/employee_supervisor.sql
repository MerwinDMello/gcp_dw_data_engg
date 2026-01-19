create or replace view `{{ params.param_hr_base_views_dataset_name }}.employee_supervisor`(employee_sid, valid_from_date, supervisor_sid, valid_to_date, employee_num, supervisor_code, lawson_company_num, process_level_code, delete_ind, source_system_code, dw_last_update_date_time)
AS (select  
employee_sid,
valid_from_date,
supervisor_sid,
valid_to_date,
employee_num,
supervisor_code,
lawson_company_num,
process_level_code,
delete_ind,
source_system_code,
dw_last_update_date_time
from {{ params.param_hr_core_dataset_name }}.employee_supervisor);