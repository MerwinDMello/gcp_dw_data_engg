begin
declare
  dup_count int64;
declare
  current_ts datetime;

set
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
truncate table
  {{ params.param_hr_stage_dataset_name }}.employee_detail_wrk;
insert into
  {{ params.param_hr_stage_dataset_name }}.employee_detail_wrk (employee_detail_code,
    employee_sid,
    applicant_type_id,
    eff_from_date,
    eff_to_date,
    detail_value_alphanumeric_text,
    detail_value_num,
    detail_value_date,
    employee_num,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
select
  hre.field_key as employee_detail_code,
  coalesce(eid.employee_sid, 0) as employee_sid,
  hre.emp_app as applicant_type_id,
  date(current_ts) as eff_from_date,
  date '9999-12-31' as eff_to_date,
  a_field as detail_value_alphanumeric_text,
  cast(n_field as int64) as detail_value_num,
  d_field as detail_value_date,
  hre.employee as employee_num,
  hre.company as lawson_company_num,
  '00000' as process_level_code,
  'L' as source_system_code,
  current_ts as dw_last_update_date_time
from
  {{ params.param_hr_stage_dataset_name }}.hrempusf as hre
inner join (
  select
    employee.employee_sid,
    employee.employee_num,
    employee.lawson_company_num
  from
    {{ params.param_hr_base_views_dataset_name }}.employee
  where
    date(employee.valid_to_date) = '9999-12-31'
  group by
    1,
    2,
    3 ) as eid
on
  hre.employee = eid.employee_num
  and (hre.company) = (eid.lawson_company_num)
where
  hre.emp_app <> 1 ;

begin transaction;
update
  {{ params.param_hr_core_dataset_name }}.employee_detail as empd
set
  valid_to_date = stg.eff_from_date - interval 1 second,
  dw_last_update_date_time = current_ts
from
  {{ params.param_hr_stage_dataset_name }}.employee_detail_wrk as stg
where
  trim(empd.employee_detail_code) = trim(stg.employee_detail_code)
  and empd.employee_sid = stg.employee_sid
  and empd.applicant_type_id = stg.applicant_type_id
  and (upper(trim(coalesce(empd.detail_value_alphanumeric_text, '~'))) <> upper(trim(coalesce(stg.detail_value_alphanumeric_text, '~')))
    or empd.detail_value_num <> stg.detail_value_num
    or empd.detail_value_date <> stg.detail_value_date
    or empd.employee_num <> stg.employee_num
    or empd.lawson_company_num <> stg.lawson_company_num
    or upper(trim(coalesce(empd.process_level_code, '~'))) <> upper(trim(coalesce(stg.process_level_code, '~')))
    or empd.source_system_code <> stg.source_system_code)
  and date(empd.valid_to_date) = '9999-12-31'
  and empd.source_system_code = 'L';

insert into
  {{ params.param_hr_core_dataset_name }}.employee_detail (employee_detail_code,
    employee_sid,
    applicant_type_id,
    valid_from_date,
    valid_to_date,
    detail_value_alphanumeric_text,
    detail_value_num,
    detail_value_date,
    delete_ind,
    employee_num,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
select
  employee_detail_wrk.employee_detail_code,
  employee_detail_wrk.employee_sid,
  employee_detail_wrk.applicant_type_id,
  current_ts,
  datetime("9999-12-31 23:59:59"),
  employee_detail_wrk.detail_value_alphanumeric_text,
  employee_detail_wrk.detail_value_num,
  employee_detail_wrk.detail_value_date,
  'A' as delete_ind,
  employee_detail_wrk.employee_num,
  employee_detail_wrk.lawson_company_num,
  employee_detail_wrk.process_level_code,
  employee_detail_wrk.source_system_code,
  employee_detail_wrk.dw_last_update_date_time
from
  {{ params.param_hr_stage_dataset_name }}.employee_detail_wrk
where
  (trim(employee_detail_wrk.employee_detail_code),
    employee_detail_wrk.employee_sid,
    employee_detail_wrk.applicant_type_id,
    upper(trim(coalesce(employee_detail_wrk.detail_value_alphanumeric_text, ''))),
    upper(trim(coalesce(cast(employee_detail_wrk.detail_value_num as string), ''))),
    employee_detail_wrk.detail_value_date,
    employee_detail_wrk.lawson_company_num,
    employee_detail_wrk.process_level_code,
    employee_detail_wrk.employee_num) not in(
  select
    as struct trim(employee_detail.employee_detail_code),
    employee_detail.employee_sid,
    (employee_detail.applicant_type_id),
    upper(trim(coalesce(employee_detail.detail_value_alphanumeric_text, ''))),
    upper(trim(coalesce(cast(employee_detail.detail_value_num as string), ''))),
    employee_detail.detail_value_date,
    employee_detail.lawson_company_num,
    employee_detail.process_level_code,
    employee_detail.employee_num
  from
    {{ params.param_hr_core_dataset_name }}.employee_detail
  where
    date(valid_to_date) = '9999-12-31' ) ;
update
  {{ params.param_hr_core_dataset_name }}.employee_detail as dtl
set
  valid_to_date = current_ts - interval 1 second
where
  date(dtl.valid_to_date) = '9999-12-31'
  and (dtl.lawson_company_num,
    dtl.process_level_code,
    dtl.employee_sid,
    dtl.employee_detail_code,
    dtl.applicant_type_id) not in(
  select
    distinct as struct lawson_company_num,
    process_level_code,
    employee_sid,
    employee_detail_code,
    applicant_type_id
  from
    {{ params.param_hr_stage_dataset_name }}.employee_detail_wrk )
    and dtl.source_system_code = 'L';

update
  {{ params.param_hr_core_dataset_name }}.employee_detail as empl
set
  delete_ind = 'D',
  valid_to_date = date_sub(current_ts, interval 1 second)
where
  upper(empl.delete_ind) = 'A'
  and (empl.lawson_company_num,
    empl.employee_num,
    empl.source_system_code) not in(
  select
    distinct as struct stg.company,
    stg.employee,
    stg.source_system_code
  from
    {{ params.param_hr_stage_dataset_name }}.hrempusf as stg )
    and empl.source_system_code = 'L';

update
  {{ params.param_hr_core_dataset_name }}.employee_detail as empl
set
  delete_ind = 'A'
where
  upper(empl.delete_ind) = 'D'
  and (empl.lawson_company_num,
    empl.employee_num,
    empl.source_system_code) in(
  select
    distinct as struct stg.company,
    stg.employee,
    stg.source_system_code
  from
    {{ params.param_hr_stage_dataset_name }}.hrempusf as stg )
    and empl.source_system_code = 'L';

   set
  dup_count = (
  select
    count(*)
  from (
    select
      employee_detail_code ,
employee_sid ,applicant_type_id ,valid_from_date
    from
      {{ params.param_hr_core_dataset_name }}.employee_detail
    group by
      employee_detail_code ,
employee_sid ,applicant_type_id ,valid_from_date
    having
      count(*) > 1 ) );
if
  dup_count <> 0 then
rollback transaction; raise
using
  message = concat('duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.employee_detail ');
  else
commit transaction;
end if
  ;
end
  ;