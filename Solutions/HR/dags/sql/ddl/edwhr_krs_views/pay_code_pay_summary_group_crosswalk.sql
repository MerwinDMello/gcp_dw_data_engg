

/*==============================================================*/
/* table: pay_code_pay_summary_group_crosswalk                  */
/*==============================================================*/

/***************************************************************************************
   c u s t o m   s e c u r i t y   v i e w
****************************************************************************************/

create or replace view {{ params.param_hr_krs_views_dataset_name }}.pay_code_pay_summary_group_crosswalk( 
clock_library_code,
kronos_pay_code,
kronos_pay_code_desc,
lawson_pay_summary_group_code,
lawson_pay_code,
source_system_code,
dw_last_update_date_time)

as

select  
a.clock_library_code,
a.kronos_pay_code,
a.kronos_pay_code_desc,
a.lawson_pay_summary_group_code,
a.lawson_pay_code,
a.source_system_code,
a.dw_last_update_date_time
from {{ params.param_hr_base_views_dataset_name }}.pay_code_pay_summary_group_crosswalk a;