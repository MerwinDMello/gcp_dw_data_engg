begin
declare
  dup_count int64;
declare
  current_ts datetime;
declare
  lv_par string;
set
  lv_par = "trim(coalesce(cast(company as string),''))||\'-\'||trim(coalesce(job_class,''))";
set
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;

call `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}','jobclass',lv_par, 'Job_Class');


truncate table
  {{ params.param_hr_stage_dataset_name }}.job_class_wrk;
insert into
  {{ params.param_hr_stage_dataset_name }}.job_class_wrk (job_class_sid,
    eff_from_date,
    hr_company_sid,
    lawson_company_num,
    job_class_code,
    job_class_desc,
    eff_to_date,
    active_dw_ind,
    process_level_code,
    security_key_text,
    source_system_code,
    dw_last_update_date_time)
select
  cast(xwlk.sk as int64) as job_class_sid,
  date(current_ts) as eff_from_date,
  coalesce(hr.hr_company_sid, 0) as hr_company_sid,
  jcl.company as lawson_company_num,
  trim(jcl.job_class) as job_class_code,
  trim(jcl.description) as job_class_desc,
  date '9999-12-31' as eff_to_date,
  'Y' as active_dw_ind,
  '00000' as process_level_code,
  concat(substr('00000', 1, 5 - length(trim(cast(jcl.company as string)))), trim(cast(jcl.company as string)), '-', '00000', '-', '00000') as security_key_text,
  'L' as source_system_code,
  current_ts as dw_last_update_date_time
from
  {{ params.param_hr_stage_dataset_name }}.jobclass as jcl
inner join
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk as xwlk
on
  upper(substr(concat(trim(coalesce(cast(jcl.company as string), '')), '-', trim(coalesce(jcl.job_class, ''))), 1, 255)) = upper(xwlk.sk_source_txt)
  and upper(xwlk.sk_type) = 'JOB_CLASS'
left outer join (
  select
    hr_company.hr_company_sid,
    hr_company.lawson_company_num
  from
    {{ params.param_hr_base_views_dataset_name }}.hr_company
  where
    date(hr_company.valid_to_date) = '9999-12-31'
    and upper(hr_company.source_system_code) = 'L'
  group by
    1,
    2 ) as hr
on
  trim(cast(jcl.company as string)) = trim(cast(hr.lawson_company_num as string)) ;

begin transaction;

update
  {{ params.param_hr_core_dataset_name }}.job_class as jclass
set
  valid_to_date = date_sub(current_ts , interval 1 second),
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
from
  {{ params.param_hr_stage_dataset_name }}.job_class_wrk as stg
where
  (jclass.job_class_sid) = (stg.job_class_sid)
  and jclass.source_system_code = 'L'
  and jclass.source_system_code = stg.source_system_code
  and upper(trim(coalesce(jclass.job_class_desc, ''))) <> upper(trim(coalesce(stg.job_class_desc, '')))
  and upper(jclass.active_dw_ind) = 'Y';

update
  {{ params.param_hr_core_dataset_name }}.job_class as jclass
set
  valid_to_date = date_sub(current_ts , interval 1 second),
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
where
  upper(jclass.active_dw_ind) = 'Y'
  and jclass.source_system_code = 'L'
  and (trim(jclass.job_class_code),
    jclass.lawson_company_num,
    jclass.source_system_code) not in(
  select
    as struct trim(job_class_wrk.job_class_code),
    job_class_wrk.lawson_company_num,
    job_class_wrk.source_system_code
  from
    {{ params.param_hr_stage_dataset_name }}.job_class_wrk )
  and upper(jclass.active_dw_ind) = 'Y';


insert into
  {{ params.param_hr_core_dataset_name }}.job_class (job_class_sid,
    valid_from_date,
    hr_company_sid,
    lawson_company_num,
    job_class_code,
    job_class_desc,
    valid_to_date,
    active_dw_ind,
    security_key_text,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
select
  job_class_wrk.job_class_sid,
  current_ts,
  job_class_wrk.hr_company_sid,
  job_class_wrk.lawson_company_num,
  job_class_wrk.job_class_code,
  job_class_wrk.job_class_desc,
  datetime("9999-12-31 23:59:59"),
  job_class_wrk.active_dw_ind,
  job_class_wrk.security_key_text,
  job_class_wrk.process_level_code,
  job_class_wrk.source_system_code,
  current_ts
from
  {{ params.param_hr_stage_dataset_name }}.job_class_wrk
where
  (trim(cast(job_class_wrk.job_class_sid as string)),
    upper(trim(coalesce(job_class_wrk.job_class_desc, '')))) not in(
  select
    as struct trim(cast(job_class.job_class_sid as string)),
    upper(trim(coalesce(job_class.job_class_desc, '')))
  from
    {{ params.param_hr_core_dataset_name }}.job_class
  where
    date(job_class.valid_to_date) = '9999-12-31' and job_class.source_system_code ='L'  ) ;

set
  dup_count = (
  select
    count(*)
  from (
    select
      job_class_sid ,valid_from_date
    from
      {{ params.param_hr_core_dataset_name }}.job_class
    group by
      job_class_sid ,valid_from_date
    having
      count(*) > 1 ) );
if
  dup_count <> 0 then
rollback transaction; raise
using
  message = concat('duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.job_class ');
  else
commit transaction;
end if
  ;
end
  ;