create table if not exists {{ params.param_hr_stage_dataset_name }}.ref_pat_sat_que_dom_xwalk_stg (
domain_id int64
, domain_label string
, question_id int64
, domaingroupid int64
, domaingroupdesc string
, rpt_label string
, dw_last_update_date_time datetime

)
  ;