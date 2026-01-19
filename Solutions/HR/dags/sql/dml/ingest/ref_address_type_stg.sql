BEGIN

create temp table t1 as
select 
*
from {{ params.param_hr_stage_dataset_name }}.ref_address_type_stg;

truncate table {{ params.param_hr_stage_dataset_name }}.ref_address_type_stg ;

insert into {{ params.param_hr_stage_dataset_name }}.ref_address_type_stg
select
    addr_type_code,
    addr_type_desc,
    (CASE
    WHEN addr_type_code IN('EMP','LOC','PRS') THEN 'L' 
      ELSE
       (CASE when addr_type_code IN('NCA','SHA') THEN 'C' ELSE 'M' END) END) as source_system_code,
    datetime_trunc(current_datetime('US/Central'), SECOND)
from t1
where length(trim(addr_type_code)) > 0;

END;