CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.mh_to_hca_reason_code (
mh_reason_cd STRING
, dw_prgm_id STRING
, dw_cret_dt DATE
, dw_lst_updt_dt DATE
, hca_reason_cd STRING
, hca_action_cd STRING
, dw_last_update_date_time DATETIME NOT NULL
)
;