begin

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.ref_nursing_program_wrk;
  
  
call `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}','galen_stg',"trim(program_ver_desc)||'-C'",'Ref_Nursing_Program');

insert into {{ params.param_hr_stage_dataset_name }}.ref_nursing_program_wrk
(
    nursing_program_id,
      program_name,
      program_type_code,
      program_degree_text,
      source_system_code,
      dw_last_update_date_time
)
select distinct
cast(xwlk.sk as INT64) as nursing_program_id,
trim(stg.program_ver_desc) as program_name,
case when stg.campus_code= 9 then 'Online'  else 'Campus' end as program_type_code,
trim(stg.program_degree) as program_degree_text,
'C' as source_system_code,
--current_timestamp() as dw_last_update_date_time;
--current_datetime('US/Central')  as  dw_last_update_date_time
timestamp_trunc(current_datetime('US/Central'), SECOND) as  dw_last_update_date_time
from {{ params.param_hr_stage_dataset_name }}.galen_stg stg 
inner join {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk xwlk
on upper(trim(program_ver_desc)||'-c')= upper(trim(xwlk.sk_source_txt))
and upper(trim(xwlk.sk_type)) = 'REF_NURSING_PROGRAM'
group by 1,2,3,4,5,6
;

end;
