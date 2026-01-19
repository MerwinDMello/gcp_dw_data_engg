begin

DECLARE
  current_ts datetime;
  set current_ts = timestamp_trunc( current_datetime('US/Central'), SECOND);


-- delete prior snapshot

delete
from {{ params.param_hr_core_dataset_name }}.candidate_pathway_microstep_snapshot where snapshot_date < date_add(current_date('US/Central'), interval -13 month);


--assigns each candidate to a pathway and loads only the microsteps for that specific pathway. a candidate can only have one pathway.
insert into {{ params.param_hr_core_dataset_name }}.candidate_pathway_microstep_snapshot (

candidate_profile_sid,
pathway_id,
microstep_num,
snapshot_date,
microstep_start_date_time,
microstep_end_date_time,
source_system_code,
dw_last_update_date_time)

select 
candidate_profile_sid,
pathway_id,
microstep_num,
current_date('US/Central') as snapshot_date,
microstep_start_date_time,
microstep_end_date_time,
source_system_code,
current_ts 

from {{ params.param_hr_base_views_dataset_name }}.candidate_pathway_microstep;

end
