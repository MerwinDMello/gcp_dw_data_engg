

/***************************************************************************************
   c u s t o m   s e c u r i t y   v i e w
****************************************************************************************/

create or replace view {{ params.param_hr_krs_views_dataset_name }}.time_entry( 
employee_num,
kronos_num,
clock_library_code,
valid_from_date,
valid_to_date,
clock_code,
clock_in_time,
clock_out_time,
clocked_hour_num,
rounded_clock_in_time,
rounded_clock_out_time,
rounded_clocked_hour_num,
time_approval_date_time,
time_approver_34_login_code,
scheduled_shift_date_time,
pay_period_start_date_time,
pay_period_end_date_time,
pay_type_code,
long_meal_code,
other_dept_code,
out_of_pay_period_code,
short_meal_code,
dept_code,
posted_ind,
lawson_company_num,
process_level_code,
source_system_code,
dw_last_update_date_time)

as

select  
a.employee_num,
a.kronos_num,
a.clock_library_code,
a.valid_from_date,
a.valid_to_date,
a.clock_code,
a.clock_in_time,
a.clock_out_time,
a.clocked_hour_num,
a.rounded_clock_in_time,
a.rounded_clock_out_time,
a.rounded_clocked_hour_num,
a.time_approval_date_time,
a.time_approver_34_login_code,
a.scheduled_shift_date_time,
a.pay_period_start_date_time,
a.pay_period_end_date_time,
a.pay_type_code,
a.long_meal_code,
a.other_dept_code,
a.out_of_pay_period_code,
a.short_meal_code,
a.dept_code,
a.posted_ind,
a.lawson_company_num,
a.process_level_code,
a.source_system_code,
a.dw_last_update_date_time
from {{ params.param_hr_base_views_dataset_name }}.time_entry a

inner join {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level c
on a.process_level_code = c.process_level_code
and a.lawson_company_num = c.lawson_company_num
and c.user_id = session_user();