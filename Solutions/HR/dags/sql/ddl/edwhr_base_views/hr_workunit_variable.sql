create or replace view {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable( 
workunit_sid,
variable_name,
variable_seq_num,
valid_from_date,
valid_to_date,
workunit_num,
variable_type_num,
variable_value_text,
lawson_company_num,
process_level_code,
active_dw_ind,
source_system_code,
dw_last_update_date_time)

as 

select  
workunit_sid,
variable_name,
variable_seq_num,
valid_from_date,
valid_to_date,
workunit_num,
variable_type_num,
variable_value_text,
lawson_company_num,
process_level_code,
active_dw_ind,
source_system_code,
dw_last_update_date_time
from {{params.param_hr_core_dataset_name}}.hr_workunit_variable;
