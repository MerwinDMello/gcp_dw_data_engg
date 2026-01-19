create or replace view {{ params.param_hr_base_views_dataset_name }}.ref_nursing_school( 
nursing_school_id,
nursing_school_name,
source_system_code,
dw_last_update_date_time)
as 
select  
nursing_school_id,
nursing_school_name,
source_system_code,
dw_last_update_date_time
from {{ params.param_hr_core_dataset_name }}.ref_nursing_school;

