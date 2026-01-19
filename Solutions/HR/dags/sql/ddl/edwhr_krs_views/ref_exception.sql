

/*==============================================================*/
/* table: ref_exception                                         */
/*==============================================================*/

/***************************************************************************************
   c u s t o m   s e c u r i t y   v i e w
****************************************************************************************/

create or replace view {{ params.param_hr_krs_views_dataset_name }}.ref_exception( 
exception_code,
exception_desc,
source_system_code,
dw_last_update_date_time)

as

select  
a.exception_code,
a.exception_desc,
a.source_system_code,
a.dw_last_update_date_time
from {{ params.param_hr_base_views_dataset_name }}.ref_exception a;