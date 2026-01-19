

/*==============================================================*/
/* table: time_entry_pay_code_detail                            */
/*==============================================================*/

/***************************************************************************************
   c u s t o m   s e c u r i t y   v i e w
****************************************************************************************/

create or replace view {{ params.param_hr_krs_views_dataset_name }}.time_entry_pay_code_detail( 
employee_num,
kronos_num,
clock_library_code,
kronos_pay_code_seq_num,
valid_from_date,
valid_to_date,
kronos_pay_code,
rounded_clocked_hour_num,
pay_summary_group_code,
lawson_company_num,
process_level_code,
source_system_code,
dw_last_update_date_time)

as

select  
a.employee_num,
a.kronos_num,
a.clock_library_code,
a.kronos_pay_code_seq_num,
a.valid_from_date,
a.valid_to_date,
a.kronos_pay_code,
a.rounded_clocked_hour_num,
a.pay_summary_group_code,
a.lawson_company_num,
a.process_level_code,
a.source_system_code,
a.dw_last_update_date_time
from {{ params.param_hr_base_views_dataset_name }}.time_entry_pay_code_detail a

inner join {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level c
on a.process_level_code = c.process_level_code
and a.lawson_company_num = c.lawson_company_num
and c.user_id = session_user();