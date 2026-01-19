create table if not exists {{ params.param_hr_stage_dataset_name }}.pfimetricssummary (
pfiworkunit numeric(12,0)
, pfiactivity int64
, startdate datetime
, enddate datetime
, activityname string
, nodecaption string
, activitytype string
, prologictype int64
, actiontaken string
, pfiuserprofile string
, authenticatedactor string
, pttaskname string
, pttasktype int64
, dw_last_update_date_time datetime
)
  ;

