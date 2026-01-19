begin
create temp table t1 as
select 
*
from {{ params.param_hr_stage_dataset_name }}.ref_status_type_stg;

truncate table {{ params.param_hr_stage_dataset_name }}.ref_status_type_stg ;

insert into {{ params.param_hr_stage_dataset_name }}.ref_status_type_stg
select 
status_type_code,
status_type_desc
from t1
where length(trim(status_type_code)) > 0;
end