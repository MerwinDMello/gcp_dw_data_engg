create table if not exists {{ params.param_hr_stage_dataset_name }}.pfiworkunitvariable (
pfiworkunit numeric(12,0)
, pfiworkunitvariable string
, variabletype int64
, variablevalue string
, seqnbr int64
, dw_last_update_date_time datetime
)
  ;

