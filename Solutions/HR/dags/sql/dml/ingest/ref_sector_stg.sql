begin
create temp table t1 as
select 
*
from {{ params.param_hr_stage_dataset_name }}.ref_sector_stg;

truncate table {{ params.param_hr_stage_dataset_name }}.ref_sector_stg ;

insert into {{ params.param_hr_stage_dataset_name }}.ref_sector_stg
select 
sector_code,
sector_desc
from t1
where length(trim(sector_code)) > 0;
end;