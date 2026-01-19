
/***************************************************************************************
   c u s t o m   s e c u r i t y   v i e w
****************************************************************************************/

create or replace view {{ params.param_hr_krs_views_dataset_name }}.ref_personnel_pay_type( 
personnel_pay_type_code,
personnel_pay_type_desc,
source_system_code,
dw_last_update_date_time)

as

select  
a.personnel_pay_type_code,
a.personnel_pay_type_desc,
a.source_system_code,
a.dw_last_update_date_time
from {{ params.param_hr_base_views_dataset_name }}.ref_personnel_pay_type a;