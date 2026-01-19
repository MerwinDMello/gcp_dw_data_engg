create or replace view {{params.param_hr_base_views_dataset_name}}.hr_workunit_metric( 
workunit_sid,
activity_seq_num,
user_profile_id_text,
valid_from_date,
valid_to_date,
workunit_num,
task_name,
task_type_num,
queue_assigment_num,
action_start_date_time,
action_taken_text,
comment_text,
authenticated_author_text,
lawson_company_num,
process_level_code,
active_dw_ind,
source_system_code,
dw_last_update_date_time)

as 

select  
workunit_sid,
activity_seq_num,
user_profile_id_text,
valid_from_date,
valid_to_date,
workunit_num,
task_name,
task_type_num,
queue_assigment_num,
action_start_date_time,
action_taken_text,
comment_text,
authenticated_author_text,
lawson_company_num,
process_level_code,
active_dw_ind,
source_system_code,
dw_last_update_date_time
from {{params.param_hr_core_dataset_name}}.hr_workunit_metric;
