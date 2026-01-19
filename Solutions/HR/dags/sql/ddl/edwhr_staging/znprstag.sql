create table if not exists {{ params.param_hr_stage_dataset_name }}.znprstag (
company int64 not null
, process_level string
, hca_proc_center string
, hca_print_loc string
, hca_taleo_flag string
, vend_intfce_dt date
, hlth_grp_eff_dt date
, hca_401k_eff_dt date
, ben_crt_user string
, ben_crt_date date
, ben_crt_time time
, ben_upd_user string
, ben_upd_date date
, ben_upd_time time
, hca_acq_flag string
, ovrd_division string
, ovrd_proc_lvl string
, hca_acq_date date
, time_zone string
, hca_coid int64
, hca_unit int64
, hca_division int64
, hca_group int64
, hca_market int64
, hca_lob string
, hca_sub_lob string
, date_stamp date
, time_stamp time
, acct_id string
, hca_usr_field1 string
, aap_flag string
, hca_nbr_ack int64
, hca_acq_401_dt date
, emp_status_01 string
, emp_status_02 string
, emp_status_03 string
, emp_status_04 string
, limit_amount_01 numeric
, limit_amount_02 numeric
, limit_amount_03 numeric
, limit_amount_04 numeric
, r_days int64
, n57set2_ss_sw string
, n57set3_ss_sw string
, dw_last_update_date_time datetime not null
)
  ;
