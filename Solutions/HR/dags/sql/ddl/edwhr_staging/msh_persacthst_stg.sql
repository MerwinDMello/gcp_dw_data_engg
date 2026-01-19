CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.msh_persacthst_stg (
company NUMERIC(4,0) NOT NULL
, action_code STRING NOT NULL
, effect_date DATE NOT NULL
, action_nbr NUMERIC(4,0) NOT NULL
, employee NUMERIC(9,0) NOT NULL
, ant_end_date DATE NOT NULL
, reason_01 STRING NOT NULL
, reason_02 STRING NOT NULL
, user_id STRING NOT NULL
, date_stamp DATE NOT NULL
, action_type STRING NOT NULL
, obj_id NUMERIC(12,0) NOT NULL
, error_flag STRING NOT NULL
, pos_level NUMERIC(2,0) NOT NULL
, update_benefit STRING NOT NULL
, update_req_ded STRING NOT NULL
, edm_effect_dt DATE NOT NULL
, edm_end_date DATE NOT NULL
, upd_abs_mgmt STRING NOT NULL
, hist_corr_flag STRING NOT NULL
, action_upd STRING NOT NULL
, create_date DATE NOT NULL
, create_time NUMERIC(6,0) NOT NULL
, create_user STRING NOT NULL
, time_stamp NUMERIC(6,0) NOT NULL
, l_index STRING NOT NULL
, l_atpah_ss_sw STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
;