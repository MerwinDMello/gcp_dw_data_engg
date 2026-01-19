/***************************************************************************************
s e c u r i t y   v i e w
****************************************************************************************/

create or replace view {{ params.param_hr_krs_views_dataset_name }}.ref_clock( 
clock_code,
clock_library_code,
clock_desc,
source_system_code,
dw_last_update_date_time)
		
as

select  
a.clock_code,
a.clock_library_code,
a.clock_desc,
a.source_system_code,
a.dw_last_update_date_time
from {{ params.param_hr_base_views_dataset_name }}.ref_clock a;