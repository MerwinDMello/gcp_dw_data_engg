begin
declare
  dup_count int64;
declare
  current_ts datetime;
set
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
begin transaction;
delete
from
  {{ params.param_hr_core_dataset_name }}.patient_satisfaction_benchmark_eoq
where
  1=1;
insert into
  {{ params.param_hr_core_dataset_name }}.patient_satisfaction_benchmark_eoq (domain_id,
    measure_id_text,
    benchmark_rank_num,
    benchmark_owner_name,
    quarter_id,
    top_box_num,
    source_system_code,
    dw_last_update_date_time)
select
  psb.domain_id,
  psb.measure_id as measure_id_text,
  psb.percentile as benchmark_rank_num,
  'PE_LOB_CMS' as benchmark_owner_name,
  lu.qtr_id as quarter_id,
  psb.top_box as top_box_num,
  'H' as source_system_code,
  current_ts as dw_last_update_date_time
from
  {{ params.param_hr_stage_dataset_name }}.patient_satisfaction_benchmark_eoq as psb
inner join
  {{ params.param_pub_views_dataset_name }}.lu_date as lu
on
  lu.date_id = psb.endofperiod_date ;
set
  dup_count = (
  select
    count(*)
  from (
    select
      domain_id,
      measure_id_text,
      benchmark_rank_num,
      benchmark_owner_name,
      quarter_id
    from
      {{ params.param_hr_core_dataset_name }}.patient_satisfaction_benchmark_eoq
    group by
      domain_id,
      measure_id_text,
      benchmark_rank_num,
      benchmark_owner_name,
      quarter_id
    having
      count(*) > 1 ) );
if
  dup_count <> 0 then
rollback transaction; raise
using
  message = concat('duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.patient_satisfaction_benchmark_eoq');
  else
commit transaction;
end if
  ;
end
  ;