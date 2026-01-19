

/***************************************************************************************
   b a s e   v i e w
****************************************************************************************/
create or replace view {{ params.param_hr_base_views_dataset_name }}.ref_diversity_inclusion ( 
leadership_level_id,
match_level_num,
match_level_desc,
lob_code,
job_class_code,
job_code,
leadership_level_desc,
leadership_level_code,
leadership_role_name,
source_system_code,
dw_last_update_date_time)

as select  
leadership_level_id,
match_level_num,
match_level_desc,
lob_code,
job_class_code,
job_code,
leadership_level_desc,
leadership_level_code,
leadership_role_name,
source_system_code,
dw_last_update_date_time
from {{ params.param_hr_core_dataset_name }}.ref_diversity_inclusion;