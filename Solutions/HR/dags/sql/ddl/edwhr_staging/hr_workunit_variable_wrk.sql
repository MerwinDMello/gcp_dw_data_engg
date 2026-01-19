create table if not exists {{ params.param_hr_stage_dataset_name }}.hr_workunit_variable_wrk (
workunit_sid numeric(18,0) not null
, variable_name string not null
, variable_seq_num int64 not null
, valid_from_date datetime not null
, valid_to_date datetime
, workunit_num numeric(12,0) not null
, variable_type_num int64
, variable_value_text string
, lawson_company_num int64 not null
, process_level_code string not null
, active_dw_ind string not null
, source_system_code string not null
, dw_last_update_date_time datetime not null
)
  ;

