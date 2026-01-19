

/*==============================================================*/
/* table: personnel_time                                        */
/*==============================================================*/

/***************************************************************************************
   c u s t o m   s e c u r i t y   v i e w
****************************************************************************************/

create or replace view {{ params.param_hr_krs_views_dataset_name }}.personnel_time( 
employee_num,
process_level_code,
clock_library_code,
valid_from_date,
valid_to_date,
personnel_name,
hire_date_time,
lawson_company_num,
job_code,
dept_code,
pay_type_code,
termination_date,
employee_34_login_code,
source_system_code,
dw_last_update_date_time)

as

select  
a.employee_num,
a.process_level_code,
a.clock_library_code,
a.valid_from_date,
a.valid_to_date,
a.personnel_name,
a.hire_date_time,
a.lawson_company_num,
a.job_code,
a.dept_code,
a.pay_type_code,
a.termination_date,
a.employee_34_login_code,
a.source_system_code,
a.dw_last_update_date_time
from {{ params.param_hr_base_views_dataset_name }}.personnel_time a

inner join {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level c
on a.process_level_code = c.process_level_code
and a.lawson_company_num = c.lawson_company_num
and c.user_id = session_user();