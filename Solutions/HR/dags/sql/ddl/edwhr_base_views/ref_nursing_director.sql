create or replace view {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director( 
job_code,
director_grouping_desc,
source_system_code,
dw_last_update_date_time)

as select  
job_code,
director_grouping_desc,
source_system_code,
dw_last_update_date_time
from {{ params.param_hr_core_dataset_name }}.ref_nursing_director;
