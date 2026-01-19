create table if not exists {{ params.param_hr_stage_dataset_name }}.domn_data_cms_rfq (
time_period string
, ccn string
, coid string
, lbl string
, analysis_id int64
, rpt_label string
, n string
, top_box numeric
, rnk int64
, reporting_period_text string
, dw_last_update_date_time datetime
)
  ;
