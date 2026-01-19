

/***************************************************************************************
s e c u r i t y   v i e w
****************************************************************************************/

create or replace view {{ params.param_hr_krs_views_dataset_name }}.gl_lawson_dept_crosswalk( 
gl_company_num,
account_unit_num,
process_level_code,
valid_from_date,
valid_to_date,
coid,
unit_num,
dept_num,
lawson_company_num,
security_key_text,
company_code,
source_system_code,
dw_last_update_date_time)
                             
as

select  
a.gl_company_num,
a.account_unit_num,
a.process_level_code,
a.valid_from_date,
a.valid_to_date,
a.coid,
a.unit_num,
a.dept_num,
a.lawson_company_num,
a.security_key_text,
a.company_code,
a.source_system_code,
a.dw_last_update_date_time
from {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk a;