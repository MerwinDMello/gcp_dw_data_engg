create or replace view {{ params.param_hr_base_views_dataset_name }}.employee_requisition_comment
AS SELECT 
employee_sid,
requisition_sid,
applicant_type_id,
comment_type_code,
action_code,
comment_line_num,
sequence_num,
hr_company_sid,
valid_from_date,
lawson_company_num,
valid_to_date,
comment_text,
comment_date,
print_code,
process_level_code,
requisition_num,
employee_num,
delete_ind,
source_system_code,
dw_last_update_date_time
from {{ params.param_hr_core_dataset_name }}.employee_requisition_comment;