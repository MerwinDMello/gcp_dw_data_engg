

/*==============================================================*/
/* table: ref_pay_summary_group                                 */
/*==============================================================*/

/***************************************************************************************
   c u s t o m   s e c u r i t y   v i e w
****************************************************************************************/

create or replace view {{ params.param_hr_krs_views_dataset_name }}.ref_pay_summary_group( 
pay_summary_group_code,
lawson_company_num,
pay_summary_group_desc,
pay_summary_abbreviation_desc,
overtime_eligibility_pay_ind,
overtime_eligibility_hour_ind,
source_system_code,
dw_last_update_date_time)

as

select  
a.pay_summary_group_code,
a.lawson_company_num,
a.pay_summary_group_desc,
a.pay_summary_abbreviation_desc,
a.overtime_eligibility_pay_ind,
a.overtime_eligibility_hour_ind,
a.source_system_code,
a.dw_last_update_date_time
from {{ params.param_hr_base_views_dataset_name }}.ref_pay_summary_group a;