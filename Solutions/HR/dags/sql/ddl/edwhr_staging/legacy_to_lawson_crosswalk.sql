create table if not exists {{ params.param_hr_stage_dataset_name }}.legacy_to_lawson_crosswalk (
acct_unit string
, cmpy int64
, dw_cret_dt date
, dw_lst_updt_dt date
, dw_prgm_id string
, hca_dept int64
, prcs_lvl string
, hca_coid int64
, hca_unit int64
, dw_last_update_date_time datetime not null
)
  ;
