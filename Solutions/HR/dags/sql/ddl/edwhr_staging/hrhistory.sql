create table if not exists {{ params.param_hr_stage_dataset_name }}.hrhistory (
seq_nbr int64 not null
, beg_date date not null
, fld_nbr int64 not null
, obj_id numeric not null
, employee int64 not null
, company int64 not null
, act_obj_id numeric
, a_value string
, currency_code string
, curr_nd int64
, data_type string
, date_stamp date
, d_value date
, hrhset4_ss_sw string
, n_value numeric
, pos_level int64
, time_stamp time
, user_id string
, source_system_code string not null
, dw_last_update_date_time datetime not null
)
  ;
