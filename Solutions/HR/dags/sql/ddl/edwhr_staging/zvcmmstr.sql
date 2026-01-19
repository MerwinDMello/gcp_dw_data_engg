create table if not exists {{ params.param_hr_stage_dataset_name }}.zvcmmstr (
hca_coid int64 not null
, per_end_date date not null
, company int64
, hca_ap_date date
, hca_corp_lvl int64
, hca_division int64
, hca_fax_area string
, hca_fax_exc string
, hca_fax_ext string
, hca_fax_stat string
, hca_flevel int64
, hca_gl_date date
, hca_group int64
, hca_hr_date date
, hca_lob string
, hca_market int64
, hca_mars_date date
, hca_medicar_no string
, hca_naics string
, hca_name10 string
, hca_name20 string
, hca_name30 string
, hca_name40 string
, hca_pas_coid int64
, hca_phone_area string
, hca_phone_exc string
, hca_phone_ext string
, hca_phone_stat string
, hca_pr_date date
, hca_sector int64
, hca_sic_code string
, hca_status string
, hca_sub_lob string
, hca_unit int64
, name string
, source_system_code string not null
, dw_last_update_date_time datetime not null
)
  ;
