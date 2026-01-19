begin
declare
  dup_count int64;
declare
  current_ts datetime;
declare
  lv_par string;
set
  lv_par = "trim(coalesce(cast(Company as string),''))  ||\'-\'|| trim(coalesce(R_Schedule ,''))";
set
  current_ts = timestamp_trunc(current_datetime('US/Central'), second) ;
  
call
  `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}',
    'prsaghead',
    lv_par,
    'Company_Pay_Schedule');
  
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.company_pay_schedule_wrk1;
insert into
  {{ params.param_hr_stage_dataset_name }}.company_pay_schedule_wrk1 (sk,
    eff_from_date,
    valid_from_date,
    hr_company_sid,
    company,
    pay_schedule_code,
    pay_schedule_flag,
    pay_schedule_eff_date,
    pay_schedule_desc,
    salary_class_flag,
    last_grade_sequence_num,
    pay_schedule_status_ind,
    currency_code,
    currency_nd,
    security_key_text,
    eff_to_date,
    active_dw_ind,
    source_system_code,
    dw_last_update_date_time)
select
  xwlk.sk,
  pr.effect_date as eff_from_date,
  current_ts as valid_from_date,
  cps.hr_company_sid,
  pr.company,
  pr.r_schedule,
  pr.r_indicator,
  pr.effect_date as effect_date,
  pr.description,
  pr.salary_class,
  pr.lst_grade_seq,
  pr.status,
  pr.currency_code,
  pr.curr_nd,
  concat(substr('00000', 1, 5 - length(trim(cast(pr.company as string)))), trim(cast(pr.company as string)), '-00000-00000') as security_key_text,
  cast('9999-12-31' as date) as eff_to_date,
  'Y' as active_ind,
  'L' as source_system_code,
  current_ts as dw_last_update_date_time
from
  {{ params.param_hr_stage_dataset_name }}.prsaghead as pr
inner join
  {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk as xwlk
on
  upper(substr(concat(trim(coalesce(cast(pr.company as string), '')), '-', trim(coalesce(pr.r_schedule, ''))), 1, 255)) = upper(xwlk.sk_source_txt)
  and upper(xwlk.sk_type) = 'COMPANY_PAY_SCHEDULE'
left outer join (
  select
    hr_company.lawson_company_num,
    hr_company.hr_company_sid
  from
    {{ params.param_hr_base_views_dataset_name }}.hr_company qualify row_number() over (partition by hr_company.lawson_company_num order by hr_company.valid_to_date desc) = 1 ) as cps
on
  pr.company = cps.lawson_company_num ;
  
begin transaction;
update
  {{ params.param_hr_core_dataset_name }}.company_pay_schedule as cps
set
  valid_to_date = current_ts - interval 1 second,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
from
  {{ params.param_hr_stage_dataset_name }}.company_pay_schedule_wrk1 as stg
where
  cps.company_pay_schedule_sid = stg.sk
  and cps.pay_schedule_eff_date = stg.pay_schedule_eff_date
  and cps.source_system_code = stg.source_system_code
  and (trim(cast(cps.hr_company_sid as string)) <> trim(coalesce(cast(stg.hr_company_sid as string), ''))
    or trim(cps.pay_schedule_flag) <> trim(stg.pay_schedule_flag)
    or trim(cps.pay_schedule_code) <> trim(stg.pay_schedule_code)
    or trim(cast(cps.lawson_company_num as string)) <> trim(cast(stg.company as string))
    or upper(trim(coalesce(cps.pay_schedule_desc, ''))) <> upper(trim(coalesce(stg.pay_schedule_desc, '')))
    or upper(trim(coalesce(cps.salary_class_flag, ''))) <> upper(trim(coalesce(stg.salary_class_flag, '')))
    or upper(trim(coalesce(cast(cps.last_grade_sequence_num as string), ''))) <> upper(trim(coalesce(cast(stg.last_grade_sequence_num as string), '')))
    or upper(trim(coalesce(cast(cps.pay_schedule_status_ind as string), ''))) <> upper(trim(coalesce(cast(stg.pay_schedule_status_ind as string), '')))
    or upper(trim(coalesce(cps.currency_code, ''))) <> upper(trim(coalesce(stg.currency_code, '')))
    or upper(trim(coalesce(cast(cps.currency_nd as string), ''))) <> upper(trim(coalesce(cast(stg.currency_nd as string), ''))))
  and cps.source_system_code = 'L';

update
  {{ params.param_hr_core_dataset_name }}.company_pay_schedule as tgt
set
  valid_to_date = current_ts - interval 1 second,
  active_dw_ind = 'N',
  dw_last_update_date_time = current_ts
where
  upper(tgt.active_dw_ind) = 'Y'
  and (tgt.lawson_company_num,
    trim(tgt.pay_schedule_code),
    trim(tgt.pay_schedule_flag),
    tgt.pay_schedule_eff_date) not in(
  select
    as struct stg.company,
    trim(stg.r_schedule),
    trim(stg.r_indicator),
    stg.effect_date
  from
    {{ params.param_hr_stage_dataset_name }}.prsaghead as stg )
    and tgt.source_system_code = 'L';

insert into
  {{ params.param_hr_core_dataset_name }}.company_pay_schedule (company_pay_schedule_sid,
    eff_from_date,
    valid_from_date,
    hr_company_sid,
    lawson_company_num,
    pay_schedule_code,
    pay_schedule_flag,
    pay_schedule_eff_date,
    pay_schedule_desc,
    salary_class_flag,
    last_grade_sequence_num,
    pay_schedule_status_ind,
    currency_code,
    currency_nd,
    security_key_text,
    valid_to_date,
    active_dw_ind,
    process_level_code,
    source_system_code,
    dw_last_update_date_time)
select
  cast(stg.sk as int64),
  stg.eff_from_date,
  current_ts,
  stg.hr_company_sid,
  stg.company,
  stg.pay_schedule_code,
  stg.pay_schedule_flag,
  stg.pay_schedule_eff_date,
  stg.pay_schedule_desc,
  stg.salary_class_flag,
  stg.last_grade_sequence_num,
  stg.pay_schedule_status_ind,
  stg.currency_code,
  stg.currency_nd,
  stg.security_key_text,
  datetime("9999-12-31 23:59:59"),
  stg.active_dw_ind,
  '00000' as process_level_code,
  stg.source_system_code,
  current_ts
from
  {{ params.param_hr_stage_dataset_name }}.company_pay_schedule_wrk1 as stg
where
  ( cast(stg.sk as string),
    trim(coalesce(cast(stg.hr_company_sid as string), '')),
    trim(cast(stg.company as string)),
    trim(stg.pay_schedule_code),
    trim(stg.pay_schedule_flag),
    stg.pay_schedule_eff_date,
    upper(trim(coalesce(stg.pay_schedule_desc, ''))),
    upper(trim(coalesce(stg.salary_class_flag, ''))),
    upper(trim(coalesce(cast(stg.last_grade_sequence_num as string), ''))),
    upper(trim(coalesce(cast(stg.pay_schedule_status_ind as string), ''))),
    upper(trim(coalesce(stg.currency_code, ''))),
    upper(trim(coalesce(cast(stg.currency_nd as string), '')))) not in(
  select
    as struct trim(cast(company_pay_schedule.company_pay_schedule_sid as string)),
    trim(cast(company_pay_schedule.hr_company_sid as string)),
    trim(cast(company_pay_schedule.lawson_company_num as string)),
    trim(company_pay_schedule.pay_schedule_code),
    trim(company_pay_schedule.pay_schedule_flag),
    company_pay_schedule.pay_schedule_eff_date,
    upper(trim(coalesce(company_pay_schedule.pay_schedule_desc, ''))),
    upper(trim(coalesce(company_pay_schedule.salary_class_flag, ''))),
    upper(trim(coalesce(cast(company_pay_schedule.last_grade_sequence_num as string), ''))),
    upper(trim(coalesce(cast(company_pay_schedule.pay_schedule_status_ind as string), ''))),
    upper(trim(coalesce(company_pay_schedule.currency_code, ''))),
    upper(trim(coalesce(cast(company_pay_schedule.currency_nd as string), '')))
  from
    {{ params.param_hr_base_views_dataset_name }}.company_pay_schedule
  where
    date(valid_to_date) = '9999-12-31' ) ; /* test unique primary index constraint set in terdata */
set
  dup_count = (
  select
    count(*)
  from (
    select
      company_pay_schedule_sid,
      eff_from_date,
      valid_from_date
    from
      {{ params.param_hr_core_dataset_name }}.company_pay_schedule
    group by
      company_pay_schedule_sid,
      eff_from_date,
      valid_from_date
    having
      count(*) > 1 ) );
if
  dup_count <> 0 then
rollback transaction; raise
using
  message = concat('duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.company_pay_schedule ');
  else
commit transaction;
end if
  ;
end
  ;