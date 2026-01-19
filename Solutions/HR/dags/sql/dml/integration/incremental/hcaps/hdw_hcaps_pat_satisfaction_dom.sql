
begin
declare
  dup_count int64;
declare 
    current_ts datetime;
set 
    current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
begin transaction;

  delete from {{ params.param_hr_core_dataset_name }}.ref_patient_satisfaction_domain where 1=1;

  insert into {{ params.param_hr_core_dataset_name }}.ref_patient_satisfaction_domain (domain_id, domain_desc, domain_group_id, domain_group_desc, source_system_code, dw_last_update_date_time)
    select
        ref_pat_sat_que_dom_xwalk_stg.domain_id as domain_id,
        ref_pat_sat_que_dom_xwalk_stg.domain_label as domain_desc,
        ref_pat_sat_que_dom_xwalk_stg.domaingroupid as domain_group_id,
        ref_pat_sat_que_dom_xwalk_stg.domaingroupdesc as domain_group_desc,
        'H' as source_system_code,
        current_ts as dw_last_update_date_time
      from
        {{ params.param_hr_stage_dataset_name }}.ref_pat_sat_que_dom_xwalk_stg
      group by 1, 2, 3, 4
      qualify row_number() over (partition by domain_id order by domain_id desc) = 1
  ;


set
  dup_count = (
  select
    count(*)
  from (
    select
      domain_id
    from
      {{ params.param_hr_core_dataset_name }}.ref_patient_satisfaction_domain
    group by
      domain_id
    having
      count(*) > 1 ) );
if
  dup_count <> 0 then
rollback transaction; raise
using
  message = concat('duplicates are not allowed in the table: edwhr_copy.ref_patient_satisfaction_domain');
  else
commit transaction;
end if
  ;
end;